"""Build and publish the curated star for a completed consumer run."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory

from .services import Reporter, Runs, clients, database

MODELS = ("dim_income_level", "dim_country", "dim_indicator", "dim_year", "fact_indicator_year")
PROJECT = "/usr/app/curated"


def select_stage(runs, invocation):
    if invocation["consumer_status"] != "complete":
        raise ValueError("consumer run is not complete")
    mode = invocation["mode"]
    if mode in ("extract", "replay"):
        source = invocation
    elif mode == "reprocess_commit":
        source_id = invocation.get("source_ingestion_id")
        if not source_id or source_id == invocation["ingestion_id"]:
            raise ValueError("invalid reprocess source")
        source = runs.get(source_id)
    else:
        raise ValueError("unsupported run mode")
    if (not source or source["mode"] not in ("extract", "replay") or
            source["stage_status"] != "complete"):
        raise ValueError("stage run is not complete")
    timestamp = source["stage_ingested_at"]
    if (not isinstance(timestamp, datetime) or timestamp.tzinfo is None or
            timestamp.utcoffset() != timezone.utc.utcoffset(timestamp)):
        raise ValueError("invalid stage timestamp")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", source["ingestion_id"], re.ASCII):
        raise ValueError("invalid stage ID")
    return source


def validate_artifacts(manifest_bytes, catalog_bytes):
    try:
        manifest, catalog = json.loads(manifest_bytes), json.loads(catalog_bytes)
        nodes, catalog_nodes = manifest["nodes"], catalog["nodes"]
        if catalog.get("errors"):
            raise ValueError("catalog has errors")
        for model in MODELS:
            node_id = "model.world_bank_curated." + model
            node, entry = nodes[node_id], catalog_nodes[node_id]
            declared, columns = node["columns"], entry["columns"]
            if not declared or not columns or not entry["metadata"].get("comment"):
                raise ValueError("incomplete catalog")
            if any(name.lower() not in {key.lower() for key in columns} for name in declared):
                raise ValueError("missing catalog column")
            if any(not value.get("type") or not value.get("comment") for value in columns.values()):
                raise ValueError("missing catalog column metadata")
        dependencies = set(nodes["model.world_bank_curated.fact_indicator_year"]["depends_on"]["nodes"])
        required = {"source.world_bank_curated.stg.observation"} | {
            "model.world_bank_curated." + name for name in ("dim_country", "dim_indicator", "dim_year")}
        if not required <= dependencies:
            raise ValueError("missing model lineage")
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("invalid dbt artifacts") from error


def build_artifacts(database_name, target):
    environment = dict(os.environ, WORLD_BANK_CURATED_DATABASE=database_name,
                       DBT_SEND_ANONYMOUS_USAGE_STATS="false", DO_NOT_TRACK="1",
                       DBT_LOG_PATH=target)
    args = ["--project-dir", PROJECT, "--profiles-dir", PROJECT, "--target-path", target]
    for command in (["dbt", "build"], ["dbt", "docs", "generate"]):
        result = subprocess.run(command + args, env=environment, capture_output=True, text=True)
        if result.returncode:
            raise ValueError("dbt command failed")
    manifest = Path(target, "manifest.json").read_bytes()
    catalog = Path(target, "catalog.json").read_bytes()
    validate_artifacts(manifest, catalog)
    return manifest, catalog


def publish(s3, ingestion_id, stage_time, manifest, catalog):
    prefix = f"world-bank/dbt_catalog/date={stage_time.date().isoformat()}/ingestion_id={ingestion_id}/"
    for name, body in (("manifest.json", manifest), ("catalog.json", catalog)):
        s3.put_object(Bucket="lakehouse-curated", Key=prefix + name, Body=body, ContentType="application/json")


def process(args):
    reporter, runs = None, None
    accepted = False
    try:
        database_name = database()
        s3, logs, sns = clients()
        reporter = Reporter(logs, sns, args.ingestion_id, os.environ["WORLD_BANK_ALERT_TOPIC_ARN"])
        reporter.open()
        reporter.event("INFO", "curated_started")
        runs = Runs(args.ingestion_id, database_name)
        invocation = runs.get(args.ingestion_id)
        if not invocation:
            raise ValueError("curated run not found")
        accepted = True
        if invocation["curated_status"] == "complete":
            print(json.dumps(reporter.event("INFO", "curated_already_complete")))
            return 0
        source = select_stage(runs, invocation)
        runs.status("pending")
        with TemporaryDirectory(prefix="world-bank-dbt-") as target:
            manifest, catalog = build_artifacts(database_name, target)
            publish(s3, args.ingestion_id, source["stage_ingested_at"], manifest, catalog)
        runs.status("complete")
        print(json.dumps(reporter.event("INFO", "curated_completed", stage_ingestion_id=source["ingestion_id"])))
        return 0
    except Exception as error:
        if runs and accepted:
            try:
                runs.status("failed")
            except Exception:
                pass
        safe_error = f"{type(error).__name__}: curated operation failed"
        if reporter:
            reporter.failure(safe_error)
        else:
            print(json.dumps(dict(level="ERROR", event="curated_failed", ingestion_id=args.ingestion_id, error=safe_error)), file=sys.stderr)
        return 1
    finally:
        if runs:
            runs.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion-id", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", args.ingestion_id, re.ASCII):
        parser.error("invalid ingestion ID")
    return process(args)


if __name__ == "__main__":
    raise SystemExit(main())
