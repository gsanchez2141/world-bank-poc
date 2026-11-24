"""Standalone extract and replay command."""
from datetime import datetime, timezone
import json
import os
import sys

import requests

from . import raw, source
from .services import Reporter, Runs, clients


def scope_from(row):
    return dict(countries=row["request_countries"], indicators=row["request_indicators"],
                start_year=row["start_year"], end_year=row["end_year"])


def run(args, scope):
    reporter = None
    runs = None
    accepted = False
    committed = False
    try:
        base = source.api_base(os.environ.get("WORLD_BANK_TEST_API_URL"))
        s3, logs, sns = clients()
        topic = os.environ["WORLD_BANK_ALERT_TOPIC_ARN"]
        reporter = Reporter(logs, sns, args.ingestion_id, topic)
        reporter.open_stream()
        reporter.event("INFO", "ingest_started")
        runs = Runs(args.ingestion_id)
        existing = runs.get(args.ingestion_id)
        if args.mode == "replay":
            source_row = runs.get(args.source_ingestion_id)
            if not source_row or source_row["mode"] != "extract" or source_row["raw_status"] != "complete":
                raise ValueError("source extract is not complete")
            scope = scope_from(source_row)
            raw_date = source_row["raw_date"]
        else:
            raw_date = datetime.now(timezone.utc).date()
        if existing:
            if (existing["mode"] != args.mode or existing["source_ingestion_id"] != args.source_ingestion_id
                    or scope_from(existing) != scope):
                raise ValueError("ingestion ID conflicts with existing scope")
            raw_date = existing["raw_date"]
            if existing["raw_status"] == "complete":
                raw.verify(s3, args.source_ingestion_id or args.ingestion_id, raw_date,
                           existing["successful_attempt"], args.mode == "replay")
                committed = True
                print(json.dumps(reporter.event("INFO", "ingest_already_complete")))
                return 0
        else:
            runs.insert(args.mode, args.source_ingestion_id, scope, raw_date)
        accepted = True
        if args.mode == "replay":
            raw.verify(s3, args.source_ingestion_id, source_row["raw_date"], source_row["successful_attempt"], True)
            reporter.event("INFO", "raw_artifacts_ready")
            runs.complete(source_row["successful_attempt"], source_row["raw_ingested_at"], source_row["raw_date"])
        else:
            attempt = runs.reserve()
            session = requests.Session()
            session.trust_env = False
            writers = []
            for table, requests_list in source.requests_for(scope, base):
                writer = raw.TableWriter(s3, table, str(raw_date), args.ingestion_id, attempt)
                writers.append(writer)
                for ordinal, (url, params) in enumerate(requests_list, 1):
                    page = 1
                    pages = None
                    while True:
                        body, actual_url = source.fetch(session, url, params, page, reporter.event)
                        actual_pages, records = source.validate_page(body, page, pages, table)
                        writer.page(ordinal, page, body, records, actual_url)
                        pages = actual_pages
                        if page >= pages:
                            break
                        page += 1
                writer.flush()
            for writer in writers:
                writer.publish()
            reporter.event("INFO", "raw_artifacts_ready")
            runs.complete(attempt, max(writer.latest for writer in writers))
        committed = True
        print(json.dumps(reporter.event("INFO", "ingest_completed")))
        return 0
    except Exception as error:
        if runs and accepted and not committed:
            try:
                runs.fail()
            except Exception:
                pass
        safe_error = f"{type(error).__name__}: ingest operation failed"
        if reporter:
            reporter.failure(safe_error)
        else:
            print(json.dumps(dict(level="ERROR", event="ingest_failed", ingestion_id=args.ingestion_id, error=safe_error)), file=sys.stderr)
        return 1
    finally:
        if runs:
            runs.close()


def main():
    args, scope = source.parse_args()
    return run(args, scope)


if __name__ == "__main__":
    raise SystemExit(main())
