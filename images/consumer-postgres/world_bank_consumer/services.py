"""Consumer warehouse transactions and local reporting."""
import json
import re
import sys
import time
import os
from pathlib import Path

from botocore.exceptions import ClientError
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from world_bank_stage.iceberg import fields
from world_bank_stage.services import clients

TABLES = ("observation", "country", "indicator", "income_level")
GROUP = "/world-bank/lakehouse-consumer-postgres"


def namespace():
    value = os.environ.get("WORLD_BANK_CONSUMER_TEST_NAMESPACE")
    if value is None:
        return "world_bank"
    if not re.fullmatch(r"stage_test_[0-9a-f]{32}", value):
        raise ValueError("invalid consumer test namespace")
    return value


class Reporter:
    def __init__(self, logs, sns, ingestion_id, topic):
        self.logs, self.sns = logs, sns
        self.ingestion_id, self.topic = ingestion_id, topic
        self.token = None

    def open(self):
        try:
            self.logs.create_log_stream(logGroupName=GROUP, logStreamName=self.ingestion_id)
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                raise

    def event(self, level, event, **extra):
        message = dict(level=level, event=event, ingestion_id=self.ingestion_id, **extra)
        request = dict(logGroupName=GROUP, logStreamName=self.ingestion_id,
                       logEvents=[dict(timestamp=int(time.time() * 1000), message=json.dumps(message))])
        if self.token:
            request["sequenceToken"] = self.token
        try:
            result = self.logs.put_log_events(**request)
        except ClientError as error:
            token = error.response.get("expectedSequenceToken")
            if not token:
                raise
            request["sequenceToken"] = token
            result = self.logs.put_log_events(**request)
        self.token = result.get("nextSequenceToken")
        return message

    def failure(self, error):
        print(json.dumps(dict(level="ERROR", event="consumer_failed", ingestion_id=self.ingestion_id, error=error)), file=sys.stderr)
        try:
            self.event("ERROR", "consumer_failed", error=error)
        except Exception:
            pass
        try:
            response = self.sns.publish(TopicArn=self.topic, Message=json.dumps(
                dict(ingestion_id=self.ingestion_id, task_name="lakehouse-consumer-postgres", error=error)))
            try:
                self.event("INFO", "alert_published", message_id=response["MessageId"])
            except Exception:
                pass
        except Exception:
            pass


class ConsumerRuns:
    def __init__(self, ingestion_id, namespace_name):
        database = namespace_name if namespace_name.startswith("stage_test_") else "warehouse"
        self.db = psycopg.connect(host="postgres", dbname=database, user="postgres",
                                  password="postgres", row_factory=dict_row)
        self.id = ingestion_id
        result = self.db.execute(
            "SELECT pg_try_advisory_lock(hashtextextended('world_bank_consumer_writer', 0)) AS ok"
        ).fetchone()
        if not result["ok"]:
            self.db.close()
            raise ValueError("another consumer is active")
        self.db.commit()

    def get(self, run_id):
        return self.db.execute("SELECT * FROM meta.pipeline_run WHERE ingestion_id=%s", (run_id,)).fetchone()

    def replace(self, changes):
        with self.db.transaction():
            self.db.execute(Path(__file__).with_name("staging.sql").read_text())
            for table in TABLES:
                self.db.execute(sql.SQL("DELETE FROM stg.{} WHERE consumer_run_id=%s").format(sql.Identifier(table)), (self.id,))
                columns = ["consumer_run_id", "change_kind", "snapshot_id"] + [name for name, _ in fields(table)]
                statement = sql.SQL("INSERT INTO stg.{} ({}) VALUES ({})").format(
                    sql.Identifier(table), sql.SQL(",").join(map(sql.Identifier, columns)),
                    sql.SQL(",").join(sql.Placeholder() for _ in columns))
                for row in changes[table]:
                    self.db.execute(statement, [self.id] + [row[name] for name in columns[1:]])
            self.db.execute("UPDATE meta.pipeline_run SET consumer_status='complete' WHERE ingestion_id=%s", (self.id,))
        self.db.commit()

    def fail(self):
        self.db.rollback()
        self.db.execute("UPDATE meta.pipeline_run SET consumer_status='failed' WHERE ingestion_id=%s AND consumer_status != 'complete'", (self.id,))
        self.db.commit()

    def close(self):
        self.db.close()
