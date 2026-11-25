"""Supervise local writer images and report failures on behalf of Airflow."""
import configparser
from contextlib import closing
import json
import logging
import os
from pathlib import Path
import re
import time
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import docker
from docker.types import Mount
from airflow.providers.docker.operators.docker import DockerOperator

from .gate import connect, load_run, fallback_id

LOG = logging.getLogger(__name__)
GROUP = "/world-bank/airflow"
IMAGES = {
    "ingest": "lakehouse-ingest-world-bank",
    "stage": "lakehouse-stage-world-bank",
    "consumer": "lakehouse-consumer-postgres",
    "curated": "dbt-postgres",
    "export": "dbt-postgres",
}
STATUSES = {"ingest": "raw_status", "stage": "stage_status", "consumer": "consumer_status", "curated": "curated_status", "export": "export_status"}


def local_clients():
    ca = Path("/opt/emulator/ca.pem")
    credentials = Path("/opt/emulator/emulator_credentials")
    if not ca.is_file() or not credentials.is_file():
        raise ValueError("emulator certificate or credentials are missing")
    parser = configparser.RawConfigParser()
    parser.read(credentials)
    session = boto3.session.Session(
        aws_access_key_id=parser.get("default", "aws_access_key_id"),
        aws_secret_access_key=parser.get("default", "aws_secret_access_key"), region_name="us-east-1")
    config = Config(region_name="us-east-1", proxies={}, inject_host_prefix=False,
                    retries={"max_attempts": 2})
    return tuple(session.client(service, endpoint_url="https://127.0.0.1:9297", verify=str(ca), config=config)
                 for service in ("logs", "sns"))


def topic_arn(sns):
    found = []
    for page in sns.get_paginator("list_topics").paginate():
        found.extend(item["TopicArn"] for item in page["Topics"] if item["TopicArn"].rsplit(":", 1)[-1] == "world-bank-alerts")
    if len(found) != 1:
        raise ValueError("local alert topic is missing or ambiguous")
    return found[0]


def fixture_url():
    value = os.environ.get("WORLD_BANK_TEST_API_URL")
    if not value:
        return None
    parsed = urlparse(value)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.username or
            parsed.password or parsed.query or parsed.fragment):
        raise ValueError("invalid fixture URL")
    return value


def job_arguments(job, row):
    args = ["--ingestion-id", row["ingestion_id"]]
    if job == "ingest":
        args.extend(["--mode", row["mode"]])
        if row["mode"] == "replay":
            args.extend(["--source-ingestion-id", row["source_ingestion_id"]])
        else:
            args.extend(["--countries", ",".join(row["request_countries"]),
                         "--indicators", ",".join(row["request_indicators"]),
                         "--start-year", str(row["start_year"]), "--end-year", str(row["end_year"])])
    return args


def execute_job(job, **context):
    ingestion_id = context["ti"].xcom_pull(task_ids="gate")
    if not isinstance(ingestion_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", ingestion_id, re.ASCII):
        raise ValueError("invalid gate result")
    with closing(connect()) as connection:
        row = load_run(connection, ingestion_id)
    if not row:
        raise ValueError("pipeline run is missing")
    if job in ("ingest", "stage") and row["mode"] in ("reprocess_commit", "rebuild"):
        return None
    if row[STATUSES[job]] == "complete":
        return None
    predecessor = {"stage":"raw_status", "consumer":"stage_status", "curated":"consumer_status", "export":"curated_status"}.get(job)
    if predecessor and row[predecessor] != "complete":
        if not (job == "consumer" and row["mode"] in ("reprocess_commit", "rebuild")):
            raise ValueError("pipeline predecessor is not complete")
    image = IMAGES[job]
    client = docker.DockerClient(base_url="unix://var/run/docker.sock", version="auto")
    try:
        try:
            client.images.get(image)
        except docker.errors.ImageNotFound as error:
            raise ValueError(f"local image {image} missing; run make platform") from error
    finally:
        client.close()
    _, sns = local_clients()
    environment = {"WORLD_BANK_ALERT_OWNER": "airflow", "WORLD_BANK_ALERT_TOPIC_ARN": topic_arn(sns)}
    if job == "ingest":
        value = fixture_url()
        if value:
            environment["WORLD_BANK_TEST_API_URL"] = value
    emulator = os.environ.get("WORLD_BANK_EMULATOR_CONTAINER", "world-bank-aws-emulator-1")
    host_directory = os.environ.get("WORLD_BANK_EMULATOR_HOST_DIR")
    if not host_directory or not host_directory.startswith("/"):
        raise ValueError("emulator host directory is not configured")
    mounts = [Mount(source=host_directory, target="/opt/emulator", type="bind", read_only=True)]
    options = {}
    if job == "export":
        export_dir = os.environ.get("WORLD_BANK_EXPORTS_HOST_DIR")
        if not export_dir or not os.path.isabs(export_dir) or os.path.normpath(export_dir) != export_dir:
            raise ValueError("exports host directory is not configured")
        mounts.append(Mount(source=export_dir, target="/exports", type="bind"))
        options["entrypoint"] = ["python", "-m", "world_bank_analytics"]
    operator = DockerOperator(
        task_id=f"run_{job}_container", image=image, command=job_arguments(job, row),
        docker_url="unix://var/run/docker.sock", api_version="auto",
        network_mode=f"container:{emulator}", mounts=mounts,
        environment=environment, mount_tmp_dir=False, auto_remove="force", do_xcom_push=False,
        force_pull=False, **options)
    operator.execute(context)
    with closing(connect()) as connection:
        updated = load_run(connection, ingestion_id)
    if not updated or updated[STATUSES[job]] != "complete":
        raise ValueError("child exited without completing its ledger status")
    return None


def failure_id(context):
    try:
        run = context["dag_run"]
        supplied = (run.conf or {}).get("ingestion_id")
        if isinstance(supplied, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", supplied, re.ASCII):
            return supplied
        return fallback_id(run.run_id)
    except Exception:
        return "airflow_unknown"


def task_failure(context):
    ingestion_id = context["ti"].xcom_pull(task_ids="gate") or failure_id(context)
    event = dict(level="ERROR", event="pipeline_task_failed", ingestion_id=ingestion_id,
                 task_name=context["task_instance"].task_id, airflow_run_id=context["dag_run"].run_id,
                 error="pipeline task failed")
    LOG.error("%s", json.dumps(event))
    try:
        logs, sns = local_clients()
    except Exception:
        LOG.exception("local failure clients unavailable")
        return
    try:
        try:
            logs.create_log_stream(logGroupName=GROUP, logStreamName=ingestion_id)
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                raise
        logs.put_log_events(logGroupName=GROUP, logStreamName=ingestion_id,
                            logEvents=[dict(timestamp=int(time.time()*1000), message=json.dumps(event))])
    except Exception:
        LOG.warning("pipeline failure log unavailable")
    try:
        message = json.dumps(dict(ingestion_id=ingestion_id, task_name=event["task_name"],
                                  airflow_run_id=event["airflow_run_id"], error=event["error"]))
        result = sns.publish(TopicArn=topic_arn(sns), Message=message)
        ack = dict(level="INFO", event="alert_published", ingestion_id=ingestion_id, message_id=result["MessageId"])
        logs.put_log_events(logGroupName=GROUP, logStreamName=ingestion_id,
                            logEvents=[dict(timestamp=int(time.time()*1000), message=json.dumps(ack))])
    except Exception:
        LOG.warning("pipeline failure alert unavailable")
