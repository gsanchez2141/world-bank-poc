"""Stage warehouse coordination and local failure reporting."""
import json
import os
import re
import sys
import time

from botocore.exceptions import ClientError
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from world_bank_ingest.services import clients

GROUP = "/world-bank/lakehouse-stage-world-bank"
TABLES = ("observation", "country", "indicator", "income_level")


def namespace():
    value = os.environ.get("WORLD_BANK_STAGE_TEST_NAMESPACE")
    if value is None:
        if os.environ.get("WORLD_BANK_STAGE_TEST_EXIT_AFTER_COMMIT"):
            raise ValueError("fault injection requires isolated namespace")
        return "world_bank"
    if not re.fullmatch(r"stage_test_[0-9a-f]{32}", value):
        raise ValueError("invalid test namespace")
    injection = os.environ.get("WORLD_BANK_STAGE_TEST_EXIT_AFTER_COMMIT")
    if injection and injection not in TABLES:
        raise ValueError("invalid fault injection table")
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

    def event(self, level, event, **fields):
        message = dict(level=level, event=event, ingestion_id=self.ingestion_id, **fields)
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
        message = dict(level="ERROR", event="stage_failed", ingestion_id=self.ingestion_id, error=error)
        print(json.dumps(message), file=sys.stderr)
        try:
            self.event("ERROR", "stage_failed", error=error)
        except Exception:
            pass
        if os.environ.get("WORLD_BANK_ALERT_OWNER") == "airflow":
            return
        try:
            result = self.sns.publish(TopicArn=self.topic, Message=json.dumps(dict(
                ingestion_id=self.ingestion_id, task_name="lakehouse-stage-world-bank", error=error)))
            try:
                self.event("INFO", "alert_published", message_id=result["MessageId"])
            except Exception:
                pass
        except Exception:
            pass


class StageRuns:
    def __init__(self, ingestion_id, namespace_name):
        database = namespace_name if namespace_name.startswith("stage_test_") else "warehouse"
        self.db = psycopg.connect(host="postgres", dbname=database, user="postgres",
                                  password="postgres", row_factory=dict_row)
        self.id = ingestion_id
        locked = self.db.execute(
            "SELECT pg_try_advisory_lock(hashtextextended('world_bank_stage_writer', 0)) AS ok"
        ).fetchone()["ok"]
        if not locked:
            self.db.close()
            raise ValueError("another stage writer is active")
        self.db.commit()

    def get(self, ingestion_id):
        return self.db.execute("SELECT * FROM meta.pipeline_run WHERE ingestion_id=%s", (ingestion_id,)).fetchone()

    def extracts(self):
        return self.db.execute("SELECT * FROM meta.pipeline_run WHERE mode='extract' AND raw_status='complete'").fetchall()

    def reserve(self, run):
        if run["stage_ingested_at"] is None:
            entries = {table: {"state": "pending", "plan": None} for table in TABLES}
            self.db.execute("""
                UPDATE meta.pipeline_run SET stage_status='pending',
                  stage_ingested_at=GREATEST(clock_timestamp(), COALESCE(
                      (SELECT MAX(stage_ingested_at) + interval '1 microsecond' FROM meta.pipeline_run),
                      clock_timestamp())),
                  stage_snapshot_ids=%s WHERE ingestion_id=%s
            """, (Jsonb(entries), self.id))
        else:
            self.db.execute("UPDATE meta.pipeline_run SET stage_status='pending' WHERE ingestion_id=%s", (self.id,))
        self.db.commit()
        return self.get(self.id)

    def entry(self, table, entry):
        self.db.execute("""
            UPDATE meta.pipeline_run SET stage_snapshot_ids=jsonb_set(stage_snapshot_ids, ARRAY[%s], %s::jsonb)
            WHERE ingestion_id=%s
        """, (table, Jsonb(entry), self.id))
        self.db.commit()

    def complete(self, count):
        self.db.execute("UPDATE meta.pipeline_run SET stage_status='complete',stage_changed_count=%s WHERE ingestion_id=%s",
                        (count, self.id))
        self.db.commit()

    def fail(self):
        self.db.rollback()
        self.db.execute("UPDATE meta.pipeline_run SET stage_status='failed' WHERE ingestion_id=%s AND stage_status != 'complete'",
                        (self.id,))
        self.db.commit()

    def close(self):
        self.db.close()
