"""Export current reporting views for one completed curated run."""
import argparse
import json
import os
from pathlib import Path
import re
import sys

from psycopg import sql

from world_bank_curated.__main__ import select_stage
from world_bank_curated.services import Reporter, Runs, clients, database
from . import exports, views


def options(database_name):
    config = "/usr/app/config/pipeline.yml"
    directory = "/exports"
    if database_name.startswith("curated_test_"):
        config = os.environ.get("WORLD_BANK_ANALYTICS_TEST_CONFIG", config)
        directory = os.environ.get("WORLD_BANK_ANALYTICS_TEST_EXPORTS_DIR", directory)
    return config, directory


def process(args):
    reporter, runs = None, None
    accepted = False
    try:
        database_name = database()
        config, directory = options(database_name)
        s3, logs, sns = clients()
        reporter = Reporter(logs, sns, args.ingestion_id, os.environ["WORLD_BANK_ALERT_TOPIC_ARN"])
        reporter.open()
        reporter.event("INFO", "export_started")
        runs = Runs(args.ingestion_id, database_name)
        invocation = runs.get(args.ingestion_id)
        if not invocation:
            raise ValueError("export run not found")
        accepted = True
        if invocation["export_status"] == "complete":
            print(json.dumps(reporter.event("INFO", "export_already_complete")))
            return 0
        if invocation["curated_status"] != "complete":
            raise ValueError("curated run is not complete")
        source = select_stage(runs, invocation)
        runs.export_status("pending")
        with runs.db.transaction():
            runs.db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            views.refresh(runs.db, config)
            rows = {}
            for name, (_, order) in exports.SPECS.items():
                query = sql.SQL("SELECT * FROM curated.{} ORDER BY {}").format(
                    sql.Identifier(name), sql.SQL(", ").join(map(sql.Identifier, order)))
                rows[name] = runs.db.execute(query).fetchall()
            artifacts = exports.render(rows)
        exports.publish(s3, args.ingestion_id, source["stage_ingested_at"].date().isoformat(), artifacts, directory)
        runs.export_status("complete")
        print(json.dumps(reporter.event("INFO", "export_completed")))
        return 0
    except Exception as error:
        if runs and accepted:
            try:
                runs.db.rollback()
                runs.export_status("failed")
            except Exception:
                pass
        safe_error = f"{type(error).__name__}: export operation failed"
        if reporter:
            reporter.failure(safe_error, event="export_failed")
        else:
            print(json.dumps(dict(level="ERROR", event="export_failed", ingestion_id=args.ingestion_id, error=safe_error)), file=sys.stderr)
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
