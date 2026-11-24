"""Local-only AWS clients, run storage and reporting."""
import configparser
import json
import os
from pathlib import Path
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import psycopg
from psycopg.rows import dict_row

ENDPOINT = "https://127.0.0.1:9297"
GROUP = "/world-bank/lakehouse-ingest-world-bank"


def clients():
    credentials = Path("/opt/emulator/emulator_credentials")
    ca = Path("/opt/emulator/ca.pem")
    if not credentials.is_file() or not ca.is_file():
        raise ValueError("emulator credentials or certificate missing")
    parser = configparser.RawConfigParser()
    parser.read(credentials)
    if not parser.has_section("default"):
        raise ValueError("invalid emulator credentials")
    access = parser.get("default", "aws_access_key_id")
    secret = parser.get("default", "aws_secret_access_key")
    session = boto3.session.Session(aws_access_key_id=access, aws_secret_access_key=secret,
                                    region_name="us-east-1")
    config = Config(region_name="us-east-1", s3={"addressing_style": "path"},
                    proxies={}, inject_host_prefix=False, retries={"max_attempts": 2})
    return tuple(session.client(service, endpoint_url=ENDPOINT, verify=str(ca), config=config)
                 for service in ("s3", "logs", "sns"))


class Reporter:
    def __init__(self, logs, sns, ingestion_id, topic):
        self.logs, self.sns, self.ingestion_id, self.topic = logs, sns, ingestion_id, topic
        self.token = None

    def open_stream(self):
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
        message = dict(level="ERROR", event="ingest_failed", ingestion_id=self.ingestion_id, error=error)
        print(json.dumps(message), file=sys.stderr)
        try:
            self.event("ERROR", "ingest_failed", error=error)
        except Exception:
            pass
        if os.environ.get("WORLD_BANK_ALERT_OWNER") == "airflow":
            return
        try:
            response = self.sns.publish(TopicArn=self.topic, Message=json.dumps(
                dict(ingestion_id=self.ingestion_id, task_name="lakehouse-ingest-world-bank", error=error)))
            try:
                self.event("INFO", "alert_published", message_id=response["MessageId"])
            except Exception:
                pass
        except Exception:
            pass


class Runs:
    def __init__(self, ingestion_id):
        self.connection = psycopg.connect("host=postgres port=5432 dbname=warehouse user=postgres password=postgres",
                                          row_factory=dict_row)
        self.id = ingestion_id
        with self.connection.transaction():
            self.connection.execute("SELECT pg_advisory_xact_lock(hashtext('world_bank_pipeline_run_ddl'))")
            self.connection.execute(Path(__file__).with_name("pipeline_run.sql").read_text())
        result = self.connection.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0)) AS locked", (ingestion_id,)).fetchone()
        if not result["locked"]:
            self.connection.close()
            raise ValueError("ingestion ID is already active")
        self.connection.commit()

    def get(self, ingestion_id):
        return self.connection.execute("SELECT * FROM meta.pipeline_run WHERE ingestion_id=%s", (ingestion_id,)).fetchone()

    def insert(self, mode, source_id, scope, raw_date):
        self.connection.execute(
            """INSERT INTO meta.pipeline_run
               (ingestion_id,mode,source_ingestion_id,request_countries,request_indicators,start_year,end_year,raw_date)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (self.id, mode, source_id, scope["countries"], scope["indicators"], scope["start_year"], scope["end_year"], raw_date))
        self.connection.commit()

    def reserve(self):
        row = self.connection.execute(
            """UPDATE meta.pipeline_run SET raw_attempt_count=raw_attempt_count+1,raw_status='pending'
               WHERE ingestion_id=%s RETURNING raw_attempt_count""", (self.id,)).fetchone()
        self.connection.commit()
        return row["raw_attempt_count"]

    def complete(self, attempt, timestamp, raw_date=None):
        self.connection.execute(
            """UPDATE meta.pipeline_run SET raw_status='complete',successful_attempt=%s,raw_ingested_at=%s,
               raw_date=COALESCE(%s,raw_date) WHERE ingestion_id=%s""", (attempt, timestamp, raw_date, self.id))
        self.connection.commit()

    def fail(self):
        self.connection.rollback()
        self.connection.execute("UPDATE meta.pipeline_run SET raw_status='failed' WHERE ingestion_id=%s AND raw_status != 'complete'", (self.id,))
        self.connection.commit()

    def close(self):
        self.connection.close()
