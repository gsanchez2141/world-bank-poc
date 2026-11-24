"""Load one exact stage commit into Postgres."""
import argparse
import json
import os
import re
import sys

from world_bank_stage.iceberg import Catalog
from .snapshots import read_changes
from .services import ConsumerRuns, Reporter, TABLES, clients, namespace


def select_source(runs, invocation):
    if invocation["mode"] in ("extract", "replay"):
        return invocation
    if invocation["mode"] == "reprocess_commit":
        source_id = invocation.get("source_ingestion_id")
        if not source_id or source_id == invocation["ingestion_id"]:
            raise ValueError("invalid reprocess source")
        source = runs.get(source_id)
        if not source or source["mode"] not in ("extract", "replay"):
            raise ValueError("reprocess source not found")
        return source
    raise ValueError("unsupported consumer mode")


def process(args):
    reporter = None
    runs = None
    accepted = False
    try:
        selected_namespace = namespace()
        _, logs, sns = clients()
        reporter = Reporter(logs, sns, args.ingestion_id, os.environ["WORLD_BANK_ALERT_TOPIC_ARN"])
        reporter.open()
        reporter.event("INFO", "consumer_started")
        runs = ConsumerRuns(args.ingestion_id, selected_namespace)
        invocation = runs.get(args.ingestion_id)
        if not invocation:
            raise ValueError("consumer run not found")
        accepted = True
        if invocation["consumer_status"] == "complete":
            print(json.dumps(reporter.event("INFO", "consumer_already_complete")))
            return 0
        source = select_source(runs, invocation)
        changes = read_changes(source, lambda: Catalog(selected_namespace))
        runs.replace(changes)
        counts = {kind: sum(row["change_kind"] == kind for table in TABLES for row in changes[table])
                  for kind in ("close", "append")}
        print(json.dumps(reporter.event("INFO", "consumer_completed", stage_ingestion_id=source["ingestion_id"],
                                        closed_count=counts["close"], appended_count=counts["append"])))
        return 0
    except Exception as error:
        if runs and accepted:
            try:
                runs.fail()
            except Exception:
                pass
        safe_error = f"{type(error).__name__}: consumer operation failed"
        if reporter:
            reporter.failure(safe_error)
        else:
            print(json.dumps(dict(level="ERROR", event="consumer_failed", ingestion_id=args.ingestion_id,
                                  error=safe_error)), file=sys.stderr)
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
