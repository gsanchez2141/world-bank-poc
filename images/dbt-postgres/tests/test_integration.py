from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from world_bank_ingest.services import clients
from world_bank_curated import __main__ as curated
from world_bank_curated.services import GROUP


class CuratedIntegration(unittest.TestCase):
    def test_star_catalog_failure_and_retry(self):
        database = "curated_test_" + uuid4().hex
        admin = psycopg.connect(host="postgres", dbname="postgres", user="postgres", password="postgres", autocommit=True)
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
        db = psycopg.connect(host="postgres", dbname=database, user="postgres", password="postgres", row_factory=dict_row)
        db.execute(Path("fixture/pipeline_run.sql").read_text())
        db.execute(Path("fixture/staging.sql").read_text())
        db.commit()
        s3, logs, _ = clients()
        run_ids = []
        env = dict(os.environ, WORLD_BANK_CURATED_TEST_DATABASE=database)

        def add_run(second, mode="extract", source_id=None):
            run_id = "curated_" + uuid4().hex
            run_ids.append(run_id)
            db.execute("""INSERT INTO meta.pipeline_run
                (ingestion_id, mode, source_ingestion_id, request_countries, request_indicators, start_year, end_year,
                 consumer_status, stage_status, stage_ingested_at)
                VALUES (%s,%s,%s,%s,%s,2019,2021,'complete','complete',%s)""",
                (run_id, mode, source_id, ["BRA"], ["SP.POP.TOTL"], f"2025-01-{second:02}T12:00:00Z"))
            db.commit()
            return run_id

        def add_stage(table, run_id, second, business, version=1, kind="append", old_run=None):
            stage_time = f"2025-01-{second:02}T12:00:00Z"
            prior = "2025-01-01T12:00:00Z"
            closing = kind == "close"
            raw_id = old_run if closing else run_id
            values = dict(consumer_run_id=run_id, change_kind=kind, snapshot_id=123,
                          api_payload="{}", record_hash="a"*64, raw_response_key=f"fixture/{raw_id}/response",
                          raw_record_index=0, response_sha256="b"*64, raw_object_key=f"fixture/{raw_id}/record",
                          source_url="https://fixture.example", api_page=1, raw_ingested_at="2025-01-01T00:00:00Z",
                          ingestion_id=raw_id, stage_run_id=raw_id, stage_ingested_at=prior if closing else stage_time,
                          version=version, valid_from=prior if closing else stage_time,
                          valid_to=stage_time if closing else None, is_current=not closing, **business)
            statement = sql.SQL("INSERT INTO stg.{} ({}) VALUES ({})").format(
                sql.Identifier(table), sql.SQL(",").join(map(sql.Identifier, values)),
                sql.SQL(",").join(sql.Placeholder() for _ in values))
            db.execute(statement, list(values.values()))

        def invoke(run_id, expected=0):
            result = subprocess.run([sys.executable, "-m", "world_bank_curated", "--ingestion-id", run_id],
                                    env=env, capture_output=True, text=True, timeout=240)
            self.assertEqual(result.returncode, expected, result.stderr)
            if expected == 0:
                self.assertTrue("curated_completed" in result.stdout or "curated_already_complete" in result.stdout)
            else:
                self.assertIn("curated_failed", result.stderr)
                self.assertNotIn("curated_completed", result.stdout)
            return result

        def query(statement, params=()):
            db.rollback()
            return db.execute(statement, params).fetchall()

        def artifact(run_id, day, name):
            key = f"world-bank/dbt_catalog/date={day}/ingestion_id={run_id}/{name}"
            return s3.get_object(Bucket="lakehouse-curated", Key=key)["Body"].read()

        def hash_key(value):
            return int.from_bytes(hashlib.md5(str(value).encode()).digest()[:8], "big", signed=True)

        try:
            seed = add_run(1)
            add_stage("income_level", seed, 1, dict(income_level_id="UMC", income_level_name="Upper middle income"))
            add_stage("income_level", seed, 1, dict(income_level_id="HIC", income_level_name="High income"))
            add_stage("country", seed, 1, dict(country_iso3="BRA", country_name="Brazil", income_level_id="UMC"))
            add_stage("indicator", seed, 1, dict(indicator_code="SP.POP.TOTL", indicator_name="Population, total"))
            add_stage("observation", seed, 1, dict(country_iso3="BRA", indicator_code="SP.POP.TOTL", year=2019, value=Decimal("10")))
            add_stage("observation", seed, 1, dict(country_iso3="BRA", indicator_code="SP.POP.TOTL", year=2020, value=None))
            db.commit()
            invoke(seed)
            facts = query("SELECT * FROM curated.fact_indicator_year ORDER BY year_sk")
            self.assertEqual(len(facts), 2)
            seed_fact = next(r for r in facts if r["year_sk"] == hash_key(2019))
            null_fact = next(r for r in facts if r["year_sk"] == hash_key(2020))
            self.assertEqual(seed_fact["value"], 10)
            self.assertIsNone(null_fact["value"])
            self.assertEqual(seed_fact["country_sk"], hash_key("BRA"))
            self.assertEqual(seed_fact["indicator_sk"], hash_key("SP.POP.TOTL"))
            self.assertEqual(seed_fact["ingestion_id"], seed)
            self.assertEqual(seed_fact["raw_response_key"], f"fixture/{seed}/response")
            self.assertTrue(query("SELECT 1 FROM curated.dim_year WHERE year=2021"))
            self.assertTrue(query("SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='curated' AND c.relname='fact_indicator_year' AND obj_description(c.oid) IS NOT NULL"))
            self.assertTrue(query("SELECT 1 FROM information_schema.columns WHERE table_schema='curated' AND table_name='fact_indicator_year' AND col_description((table_schema||'.'||table_name)::regclass::oid, ordinal_position) IS NOT NULL"))
            manifest, catalog = artifact(seed, "2025-01-01", "manifest.json"), artifact(seed, "2025-01-01", "catalog.json")
            curated.validate_artifacts(manifest, catalog)
            repeat = invoke(seed)
            self.assertIn("curated_already_complete", repeat.stdout)
            self.assertEqual(len(query("SELECT * FROM curated.fact_indicator_year")), 2)

            later = add_run(2)
            add_stage("observation", later, 2, dict(country_iso3="BRA", indicator_code="SP.POP.TOTL", year=2019, value=Decimal("10")), version=1, kind="close", old_run=seed)
            precise = Decimal("12345678901234567890.123456789012345678")
            add_stage("observation", later, 2, dict(country_iso3="BRA", indicator_code="SP.POP.TOTL", year=2019, value=precise), version=2)
            add_stage("country", later, 2, dict(country_iso3="BRA", country_name="Brazil", income_level_id="HIC"), version=2)
            db.commit()
            invoke(later)
            facts = query("SELECT * FROM curated.fact_indicator_year")
            updated = next(r for r in facts if r["year_sk"] == hash_key(2019))
            self.assertEqual(updated["value"], precise)
            self.assertEqual(updated["ingestion_id"], later)
            self.assertEqual(next(r for r in facts if r["year_sk"] == hash_key(2020)), null_fact)
            self.assertEqual(query("SELECT income_level_sk FROM curated.dim_country WHERE country_iso3='BRA'")[0]["income_level_sk"], hash_key("HIC"))
            replay = add_run(3, "reprocess_commit", seed)
            for table in ("observation", "country", "indicator", "income_level"):
                columns = query("""SELECT column_name FROM information_schema.columns
                                   WHERE table_schema='stg' AND table_name=%s ORDER BY ordinal_position""", (table,))
                names = [row["column_name"] for row in columns]
                projection = [sql.Placeholder() if name == "consumer_run_id" else sql.Identifier(name) for name in names]
                statement = sql.SQL("INSERT INTO stg.{} ({}) SELECT {} FROM stg.{} WHERE consumer_run_id=%s").format(
                    sql.Identifier(table), sql.SQL(",").join(map(sql.Identifier, names)), sql.SQL(",").join(projection), sql.Identifier(table))
                db.execute(statement, (replay, seed))
                db.commit()
            invoke(replay)
            self.assertEqual(next(r for r in query("SELECT * FROM curated.fact_indicator_year") if r["year_sk"] == hash_key(2019))["value"], precise)
            self.assertEqual(query("SELECT income_level_sk FROM curated.dim_country WHERE country_iso3='BRA'")[0]["income_level_sk"], hash_key("HIC"))
            failing = add_run(4)
            add_stage("observation", failing, 4, dict(country_iso3="BRA", indicator_code="SP.POP.TOTL", year=2019, value=Decimal("77")), version=3)
            db.execute("""CREATE FUNCTION curated.fixture_reject() RETURNS trigger LANGUAGE plpgsql AS $$
                       BEGIN IF NEW.ingestion_id = TG_ARGV[0] THEN RAISE EXCEPTION 'fixture'; END IF;
                       RETURN NEW; END $$""")
            db.execute(sql.SQL("CREATE TRIGGER fixture_reject BEFORE UPDATE ON curated.fact_indicator_year FOR EACH ROW EXECUTE FUNCTION curated.fixture_reject({})").format(sql.Literal(failing)))
            db.commit()
            invoke(failing, 1)
            state = query("SELECT consumer_status,curated_status FROM meta.pipeline_run WHERE ingestion_id=%s", (failing,))[0]
            self.assertEqual((state["consumer_status"], state["curated_status"]), ("complete", "failed"))
            prefix = f"world-bank/dbt_catalog/date=2025-01-04/ingestion_id={failing}/"
            self.assertFalse(s3.list_objects_v2(Bucket="lakehouse-curated", Prefix=prefix).get("Contents"))
            events = logs.filter_log_events(logGroupName=GROUP, logStreamNames=[failing])["events"]
            messages = [json.loads(event["message"]) for event in events]
            self.assertTrue(any(m["event"] == "curated_failed" for m in messages))
            self.assertTrue(any(m["event"] == "alert_published" and m["message_id"] for m in messages))
            db.execute("DROP TRIGGER fixture_reject ON curated.fact_indicator_year")
            db.execute("DROP FUNCTION curated.fixture_reject()")
            db.commit()
            invoke(failing)
            self.assertEqual(query("SELECT curated_status FROM meta.pipeline_run WHERE ingestion_id=%s", (failing,))[0]["curated_status"], "complete")
            facts = query("SELECT * FROM curated.fact_indicator_year")
            self.assertEqual(next(r for r in facts if r["year_sk"] == hash_key(2019))["value"], 77)
            self.assertEqual(next(r for r in facts if r["year_sk"] == hash_key(2020)), null_fact)
            curated.validate_artifacts(artifact(failing, "2025-01-04", "manifest.json"),
                                       artifact(failing, "2025-01-04", "catalog.json"))
            curated.validate_artifacts(artifact(replay, "2025-01-01", "manifest.json"),
                                       artifact(replay, "2025-01-01", "catalog.json"))
        finally:
            db.close()
            prefix = "world-bank/dbt_catalog/"
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket="lakehouse-curated", Prefix=prefix):
                for item in page.get("Contents", []):
                    if any(f"/ingestion_id={run_id}/" in item["Key"] for run_id in run_ids):
                        s3.delete_object(Bucket="lakehouse-curated", Key=item["Key"])
            for run_id in run_ids:
                try:
                    logs.delete_log_stream(logGroupName=GROUP, logStreamName=run_id)
                except Exception:
                    pass
            admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(database)))
            admin.close()


if __name__ == "__main__":
    unittest.main()
