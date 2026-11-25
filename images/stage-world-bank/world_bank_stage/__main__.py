"""Standalone stage command with durable per-table recovery."""
import argparse
import json
import os
import re
import sys

from . import input as raw_input
from . import scd2
from .iceberg import Catalog
from .services import Reporter, StageRuns, TABLES, clients, namespace


def source_run(runs, run):
    if run["raw_status"] != "complete" or run["mode"] not in ("extract", "replay"):
        raise ValueError("raw run is not complete")
    if run["mode"] == "extract":
        return run
    source = runs.get(run["source_ingestion_id"])
    if not source or source["mode"] != "extract" or source["raw_status"] != "complete":
        raise ValueError("replay source is not complete")
    columns = ("raw_date", "successful_attempt", "raw_ingested_at", "request_countries",
               "request_indicators", "start_year", "end_year")
    if any(run[name] != source[name] for name in columns):
        raise ValueError("replay source coordinates do not match")
    return source


def verify_completed(catalog, entries):
    if not isinstance(entries, dict) or set(entries) != set(TABLES):
        raise ValueError("invalid stage manifest")
    for table, entry in entries.items():
        if entry.get("state") != "complete":
            raise ValueError("incomplete stage manifest")
        ids = [value for value in (entry.get("overwrite"), entry.get("append")) if value is not None]
        if ids:
            info = catalog.inspect(table)
            if info is None:
                raise ValueError("completed table is missing")
            known = {str(item["snapshot-id"]) for item in info["metadata"].get("snapshots", [])}
            if not set(ids) <= known:
                raise ValueError("completed stage snapshot is missing")


def process(args):
    reporter = None
    runs = None
    accepted = False
    try:
        namespace_name = namespace()
        s3, logs, sns = clients()
        reporter = Reporter(logs, sns, args.ingestion_id, os.environ["WORLD_BANK_ALERT_TOPIC_ARN"])
        reporter.open()
        reporter.event("INFO", "stage_started")
        runs = StageRuns(args.ingestion_id, namespace_name)
        run = runs.get(args.ingestion_id)
        if not run:
            raise ValueError("raw run does not exist")
        source = source_run(runs, run)
        accepted = True
        catalog = Catalog(namespace_name)
        if run["stage_status"] == "complete":
            verify_completed(catalog, run["stage_snapshot_ids"])
            print(json.dumps(reporter.event("INFO", "stage_already_complete")))
            return 0

        for table in TABLES:
            info = catalog.inspect(table)
            if info is not None:
                scd2.validate_history(table, catalog.rows(table, info=info))

        candidates = raw_input.read_extract(s3, source)
        candidates = raw_input.filter_fresh(s3, candidates, runs.extracts())
        run = runs.reserve(run)
        stage_time = run["stage_ingested_at"]
        entries = run["stage_snapshot_ids"]

        for table in TABLES:
            entry = entries[table]
            if entry["state"] == "complete":
                continue
            if entry["plan"] is not None:
                recovered = catalog.recover(table, entry["plan"], args.ingestion_id, stage_time)
                if recovered is not None:
                    runs.entry(table, recovered)
                    entries[table] = recovered
                    reporter.event("INFO", "stage_table_recovered", table=table, **{
                        key: recovered[key] for key in ("overwrite", "append", "changed_count")})
                    continue
        for table in TABLES:
            if entries[table]["state"] == "complete":
                continue
            info = catalog.inspect(table)
            current = [row for row in catalog.rows(table, info=info) if row["is_current"]] if info else []
            closes, appends = scd2.plan(table, candidates[table], current, stage_time)
            if not appends:
                entry = dict(state="complete", overwrite=None, append=None, changed_count=0)
                runs.entry(table, entry)
                entries[table] = entry
                reporter.event("INFO", "stage_table_unchanged", table=table, changed_count=0)
                continue
            if info is None:
                info = catalog.create(table)
            head = info["metadata"].get("current-snapshot-id")
            parent = str(head) if head not in (None, -1) else None
            plan = dict(parent=parent, closes=closes, appends=appends)
            runs.entry(table, dict(state="pending", plan=plan))
            catalog.write(table, closes, appends, candidates[table], args.ingestion_id, stage_time)
            if os.environ.get("WORLD_BANK_STAGE_TEST_EXIT_AFTER_COMMIT") == table:
                os._exit(75)
            entry = catalog.recover(table, plan, args.ingestion_id, stage_time)
            if entry is None:
                raise ValueError("committed snapshots were not attributable")
            runs.entry(table, entry)
            entries[table] = entry
            reporter.event("INFO", "stage_table_committed", table=table, **{
                key: entry[key] for key in ("overwrite", "append", "changed_count")})
        verify_completed(catalog, entries)
        for table in TABLES:
            info = catalog.inspect(table)
            if info is not None:
                scd2.validate_history(table, catalog.rows(table, info=info))
        runs.complete(sum(entry["changed_count"] for entry in entries.values()))
        print(json.dumps(reporter.event("INFO", "stage_completed")))
        return 0
    except Exception as error:
        if runs and accepted:
            try:
                runs.fail()
            except Exception:
                pass
        safe_error = f"{type(error).__name__}: stage operation failed"
        if reporter:
            reporter.failure(safe_error)
        else:
            print(json.dumps(dict(level="ERROR", event="stage_failed", ingestion_id=args.ingestion_id, error=safe_error)),
                  file=sys.stderr)
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
