"""Local-only warehouse coordination and curated failure reporting."""
import json
import os
import re
import sys
import time

import psycopg
from psycopg.rows import dict_row
from botocore.exceptions import ClientError
from world_bank_ingest.services import clients

GROUP = "/world-bank/dbt-postgres"


def database():
    name = os.environ.get("WORLD_BANK_CURATED_TEST_DATABASE")
    if name is None:
        return "warehouse"
    if not re.fullmatch(r"curated_test_[0-9a-f]{32}", name):
        raise ValueError("invalid curated test database")
    return name


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
            response = self.logs.put_log_events(**request)
        except ClientError as error:
            token = error.response.get("expectedSequenceToken")
            if not token:
                raise
            request["sequenceToken"] = token
            response = self.logs.put_log_events(**request)
        self.token = response.get("nextSequenceToken")
        return message

    def failure(self, error):
        print(json.dumps(dict(level="ERROR", event="curated_failed", ingestion_id=self.ingestion_id, error=error)), file=sys.stderr)
        try:
            self.event("ERROR", "curated_failed", error=error)
        except Exception:
            pass
        try:
            response = self.sns.publish(TopicArn=self.topic, Message=json.dumps(dict(
                ingestion_id=self.ingestion_id, task_name="dbt-postgres", error=error)))
            try:
                self.event("INFO", "alert_published", message_id=response["MessageId"])
            except Exception:
                pass
        except Exception:
            pass


class Runs:
    def __init__(self, ingestion_id, database_name):
        self.db = psycopg.connect(host="postgres", dbname=database_name, user="postgres",
                                  password="postgres", row_factory=dict_row)
        self.id = ingestion_id
        for lock_name in ("world_bank_curated_writer", "world_bank_consumer_writer"):
            row = self.db.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS ok",
                                  (lock_name,)).fetchone()
            if not row["ok"]:
                self.db.close()
                raise ValueError("a required writer is active")
        self.db.commit()

    def get(self, ingestion_id):
        return self.db.execute("SELECT * FROM meta.pipeline_run WHERE ingestion_id=%s", (ingestion_id,)).fetchone()

    def status(self, status):
        self.db.execute("UPDATE meta.pipeline_run SET curated_status=%s WHERE ingestion_id=%s AND curated_status != 'complete'",
                        (status, self.id))
        self.db.commit()

    def close(self):
        self.db.close()
