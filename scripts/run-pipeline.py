#!/usr/bin/env python3
"""Trigger one local Airflow DagRun and wait for that run's terminal state."""
import base64
import hashlib
import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, quote
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import uuid4

DAG = "world_bank_pipeline"


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def validate_url(base_url):
    parsed = urlparse(base_url)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.username or parsed.password or
            parsed.path not in ("", "/") or parsed.query or parsed.fragment or not parsed.port):
        raise ValueError("Airflow URL must be an HTTP loopback address with a port")
    return base_url.rstrip("/")


def fallback_id(run_id):
    return "airflow_" + hashlib.sha256(run_id.encode()).hexdigest()


def trigger_and_wait(base_url, conf, timeout=1800, poll_interval=3, dag=DAG):
    base_url = validate_url(base_url)
    if dag not in (DAG, "world_bank_backfill") or not isinstance(conf, dict) or timeout <= 0 or poll_interval <= 0:
        raise ValueError("invalid trigger configuration or deadline")
    opener = build_opener(ProxyHandler({}), NoRedirect())
    run_id = "manual_" + uuid4().hex
    authorization = base64.b64encode(b"airflow:airflow").decode("ascii")
    headers = {"Authorization": "Basic " + authorization, "Content-Type": "application/json"}
    collection = f"{base_url}/api/v1/dags/{dag}/dagRuns"
    body = json.dumps({"dag_run_id": run_id, "conf": conf}).encode()
    request = Request(collection, data=body, headers=headers, method="POST")
    identifier = conf.get("ingestion_id", fallback_id(run_id))
    print(f"airflow_run_id={run_id} ingestion_id={identifier}", flush=True)
    with opener.open(request, timeout=15) as response:
        created = json.load(response)
    if created.get("dag_run_id") != run_id:
        raise ValueError("Airflow returned a different run ID")
    deadline = time.monotonic() + timeout
    url = collection + "/" + quote(run_id, safe="")
    while time.monotonic() < deadline:
        with opener.open(Request(url, headers=headers), timeout=min(15, max(1, deadline-time.monotonic()))) as response:
            status = json.load(response)
        if status.get("dag_run_id") != run_id:
            raise ValueError("Airflow returned a different run ID")
        state = status.get("state")
        if state == "success":
            print("state=success")
            return 0
        if state == "failed":
            print("state=failed", file=sys.stderr)
            return 1
        time.sleep(min(poll_interval, max(0, deadline-time.monotonic())))
    print("Airflow run timed out; inspect the existing run", file=sys.stderr)
    return 1


def demo(base, timeout):
    print("Running the default writer", flush=True)
    if trigger_and_wait(base, {}, timeout=timeout):
        return 1
    conf = {
        "mode": "extract",
        "ingestion_id": "demo_backfill_" + uuid4().hex,
        "countries": ["BRA"],
        "indicators": ["SP.POP.TOTL"],
        "start_year": 2021,
        "end_year": 2021,
    }
    print("Running the 2021 Brazil population backfill", flush=True)
    if trigger_and_wait(base, conf, timeout=timeout, dag="world_bank_backfill"):
        return 1
    export_dir = os.environ.get("WORLD_BANK_EXPORTS_HOST_DIR", "exports")
    print("CSV reports:")
    for name in ("vw_missing_or_incomplete.csv", "vw_yoy_change.csv",
                 "vw_latest_year_comparison.csv", "vw_income_group_year.csv"):
        print(os.path.join(export_dir, name))
    return 0


def main():
    try:
        base = os.environ.get("WORLD_BANK_AIRFLOW_URL", "http://127.0.0.1:8080")
        timeout = float(os.environ.get("WORLD_BANK_RUN_TIMEOUT", "1800"))
        if sys.argv[1:] == ["--demo"]:
            return demo(base, timeout)
        if sys.argv[1:]:
            raise ValueError("unknown arguments")
        conf = json.loads(os.environ.get("WORLD_BANK_RUN_CONF", "{}"))
        return trigger_and_wait(base, conf, timeout=timeout)
    except (ValueError, TypeError, json.JSONDecodeError, HTTPError, URLError, OSError) as error:
        print(f"pipeline trigger failed: {type(error).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
