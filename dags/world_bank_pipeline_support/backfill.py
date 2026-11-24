"""Trigger exactly one writer run and wait for its Airflow terminal state."""
import hashlib
import logging
import time

from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import DagRunAlreadyExists
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session

from .gate import ALLOWED, fallback_id, valid_id

LOG = logging.getLogger(__name__)
WRITER = "world_bank_pipeline"
MODES = ("extract", "replay", "reprocess_commit", "rebuild")


def child_run_id(parent_run_id):
    if not isinstance(parent_run_id, str) or not parent_run_id:
        raise ValueError("missing parent run ID")
    return "manual__wbbackfill_" + hashlib.sha256(parent_run_id.encode()).hexdigest()


def child_conf(conf, run_id):
    if not isinstance(conf, dict) or set(conf) - ALLOWED:
        raise ValueError("invalid backfill configuration")
    if conf.get("mode", "extract") not in MODES:
        raise ValueError("unsupported backfill mode")
    if "ingestion_id" in conf and not valid_id(conf["ingestion_id"]):
        raise ValueError("invalid ingestion ID")
    if "source_ingestion_id" in conf and not valid_id(conf["source_ingestion_id"]):
        raise ValueError("invalid source ingestion ID")
    result = dict(conf)
    result.setdefault("ingestion_id", fallback_id(run_id))
    return result


def find_exact(run_id):
    with create_session() as session:
        matches = DagRun.find(dag_id=WRITER, run_id=[run_id], session=session)
        if len(matches) > 1:
            raise ValueError("ambiguous writer child")
        if not matches:
            return None
        run = matches[0]
        return {"dag_id": run.dag_id, "run_id": run.run_id, "conf": run.conf, "state": run.state}


def check_child(child, run_id, conf):
    if not child or child["dag_id"] != WRITER or child["run_id"] != run_id or child["conf"] != conf:
        raise ValueError("writer child is missing or conflicts")
    return child


def trigger_and_wait(**context):
    parent = context["dag_run"]
    run_id = child_run_id(parent.run_id)
    conf = child_conf(parent.conf, run_id)
    LOG.info("writer_run_id=%s ingestion_id=%s", run_id, conf["ingestion_id"])
    child = find_exact(run_id)
    if child is None:
        try:
            trigger_dag(dag_id=WRITER, run_id=run_id, conf=conf, replace_microseconds=False)
        except DagRunAlreadyExists:
            pass
        child = find_exact(run_id)
    check_child(child, run_id, conf)
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        child = check_child(find_exact(run_id), run_id, conf)
        state = child["state"]
        if state == "success":
            LOG.info("writer_run_id=%s state=success", run_id)
            return None
        if state == "failed":
            raise ValueError(f"writer run {run_id} failed")
        time.sleep(min(3, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"writer run {run_id} did not finish before the deadline")
