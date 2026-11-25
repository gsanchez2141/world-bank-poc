import csv
from decimal import Decimal
import importlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from world_bank_ingest.services import clients
from world_bank_analytics import exports


class ExportIntegration(unittest.TestCase):
    def test_real_exports_and_empty_schemas(self):
        fixture = importlib.import_module("analytics_tests.test_views").ReportingViews
        fixture.setUpClass()
        run_ids = []
        s3, logs, _ = clients()
        try:
            with tempfile.TemporaryDirectory(prefix="analytics-fixture-") as directory:
                env = dict(os.environ, WORLD_BANK_CURATED_TEST_DATABASE=fixture.database,
                           WORLD_BANK_ANALYTICS_TEST_CONFIG=fixture.config.name,
                           WORLD_BANK_ANALYTICS_TEST_EXPORTS_DIR=directory)

                def add_run(day):
                    run_id = "analytics_" + uuid4().hex
                    run_ids.append(run_id)
                    fixture.db.execute("""INSERT INTO meta.pipeline_run
                        (ingestion_id, mode, request_countries, request_indicators, start_year, end_year,
                         raw_status, consumer_status, stage_status, curated_status, stage_ingested_at)
                        VALUES (%s,'extract',ARRAY['BRA'],ARRAY['SP.POP.TOTL'],2019,2021,
                                'complete','complete','complete','complete',%s)""",
                        (run_id, f"2025-01-{day:02}T12:00:00Z"))
                    fixture.db.commit()
                    return run_id

                def invoke(run_id):
                    result = subprocess.run([sys.executable, "-m", "world_bank_analytics", "--ingestion-id", run_id],
                                            env=env, capture_output=True, text=True, timeout=120)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("export_completed", result.stdout)

                exact = Decimal("12345678901234567890.123456789012345678")
                fixture.db.execute("UPDATE curated.fact_indicator_year SET value=%s WHERE country_sk=1 AND indicator_sk=2 AND year_sk=2019", (exact,))
                fixture.db.commit()
                run_id = add_run(2)
                invoke(run_id)
                prefix = f"world-bank/reporting/date=2025-01-02/ingestion_id={run_id}/"
                for name, (columns, _) in exports.SPECS.items():
                    csv_body = s3.get_object(Bucket="lakehouse-curated", Key=prefix + name + ".csv")["Body"].read()
                    parquet_body = s3.get_object(Bucket="lakehouse-curated", Key=prefix + name + ".parquet")["Body"].read()
                    self.assertEqual(csv_body, Path(directory, name + ".csv").read_bytes())
                    table = pq.read_table(io.BytesIO(parquet_body))
                    self.assertEqual(table.schema, pa.schema([pa.field(column, kind) for column, kind in columns]))
                    csv_rows = list(csv.DictReader(io.StringIO(csv_body.decode("utf-8"))))
                    self.assertEqual(len(csv_rows), table.num_rows)
                    if name == "vw_missing_or_incomplete":
                        match = next(row for row in csv_rows if row["country_iso3"] == "BRA" and
                                     row["indicator_code"] == "SP.POP.TOTL" and row["year"] == "2020")
                        self.assertEqual((match["value"], match["reason"]), ("", "NEVER_RETURNED"))
                    if name == "vw_yoy_change":
                        match = next(row for row in table.to_pylist() if row["country_iso3"] == "BRA" and
                                     row["indicator_code"] == "NY.GDP.MKTP.CD" and row["year"] == 2019)
                        self.assertEqual(match["value"], exact)
                        self.assertIn(str(exact), csv_body.decode())
                status = fixture.db.execute("SELECT export_status FROM meta.pipeline_run WHERE ingestion_id=%s", (run_id,)).fetchone()[0]
                self.assertEqual(status, "complete")
                fixture.db.execute("DELETE FROM curated.fact_indicator_year")
                fixture.db.execute("DELETE FROM curated.dim_country")
                fixture.db.execute("DELETE FROM curated.dim_indicator")
                fixture.db.execute("DELETE FROM curated.dim_income_level")
                fixture.db.execute("DELETE FROM curated.dim_year")
                fixture.db.commit()
                empty_id = add_run(3)
                invoke(empty_id)
                prefix = f"world-bank/reporting/date=2025-01-03/ingestion_id={empty_id}/"
                for name, (columns, _) in exports.SPECS.items():
                    csv_body = s3.get_object(Bucket="lakehouse-curated", Key=prefix + name + ".csv")["Body"].read()
                    parquet_body = s3.get_object(Bucket="lakehouse-curated", Key=prefix + name + ".parquet")["Body"].read()
                    self.assertEqual(csv_body.decode(), ",".join(column for column, _ in columns) + "\n")
                    table = pq.read_table(io.BytesIO(parquet_body))
                    self.assertEqual(table.num_rows, 0)
                    self.assertEqual(table.schema, pa.schema([pa.field(column, kind) for column, kind in columns]))
        finally:
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket="lakehouse-curated", Prefix="world-bank/reporting/"):
                for item in page.get("Contents", []):
                    if any(f"/ingestion_id={run_id}/" in item["Key"] for run_id in run_ids):
                        s3.delete_object(Bucket="lakehouse-curated", Key=item["Key"])
            for run_id in run_ids:
                try:
                    logs.delete_log_stream(logGroupName="/world-bank/dbt-postgres", logStreamName=run_id)
                except Exception:
                    pass
            fixture.tearDownClass()


if __name__ == "__main__":
    unittest.main()
