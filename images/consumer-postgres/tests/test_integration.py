from decimal import Decimal
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
from psycopg.types.json import Jsonb
import simplejson

from world_bank_ingest import raw
from world_bank_ingest.services import clients
from world_bank_stage.iceberg import Catalog
from world_bank_consumer.services import GROUP


class ConsumerIntegration(unittest.TestCase):
    def test_historical_load_reprocess_and_atomic_retry(self):
        ns = "stage_test_" + uuid4().hex
        admin = psycopg.connect(host="postgres", dbname="postgres", user="postgres", password="postgres", autocommit=True)
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(ns)))
        db = psycopg.connect(host="postgres", dbname=ns, user="postgres", password="postgres", row_factory=dict_row)
        db.execute(Path("world_bank_ingest/pipeline_run.sql").read_text())
        db.commit()
        s3, logs, _ = clients()
        catalog = Catalog(ns)
        ids = []
        env = dict(os.environ, WORLD_BANK_STAGE_TEST_NAMESPACE=ns, WORLD_BANK_CONSUMER_TEST_NAMESPACE=ns)

        def publish(second, observations, country="Brazil"):
            run_id = "consumer_" + uuid4().hex
            ids.append(run_id)
            when = f"2025-01-01T00:00:{second:02d}Z"
            previous = raw.now
            raw.now = lambda: when
            try:
                tables = {
                    "observation": [{"countryiso3code":"BRA", "indicator":{"id":"SP.POP.TOTL"}, "date":str(year), "value":value}
                                    for year, value in observations],
                    "country": [{"id":"BRA", "name":country, "incomeLevel":{"id":"UMC"}}],
                    "indicator": [{"id":"SP.POP.TOTL", "name":"Population, total"}],
                    "income_level": [{"id":"UMC", "value":"Upper middle income"}],
                }
                for table, records in tables.items():
                    writer = raw.TableWriter(s3, table, "2025-01-01", run_id, 1)
                    body = simplejson.dumps([{"page":1,"pages":1}, records], use_decimal=True, separators=(",", ":")).encode()
                    writer.page(1, 1, body, records, "https://fixture.example")
                    writer.publish()
            finally:
                raw.now = previous
            db.execute("""INSERT INTO meta.pipeline_run
                (ingestion_id,mode,request_countries,request_indicators,start_year,end_year,
                 raw_status,raw_date,successful_attempt,raw_attempt_count,raw_ingested_at)
                VALUES (%s,'extract',%s,%s,2020,2021,'complete','2025-01-01',1,1,%s)""",
                (run_id, ["BRA"], ["SP.POP.TOTL"], when))
            db.commit()
            return run_id

        def reference(source_id, mode):
            run_id = "consumer_" + uuid4().hex
            ids.append(run_id)
            db.execute("""INSERT INTO meta.pipeline_run
                (ingestion_id,mode,source_ingestion_id,request_countries,request_indicators,start_year,end_year,
                 raw_status,raw_date,successful_attempt,raw_ingested_at)
                SELECT %s,%s,ingestion_id,request_countries,request_indicators,start_year,end_year,
                       raw_status,raw_date,successful_attempt,raw_ingested_at
                FROM meta.pipeline_run WHERE ingestion_id=%s""", (run_id, mode, source_id))
            db.commit()
            return run_id

        def run(module, run_id, expected=0, injection=None):
            command_env = dict(env)
            if injection:
                command_env["WORLD_BANK_STAGE_TEST_EXIT_AFTER_COMMIT"] = injection
            result = subprocess.run([sys.executable, "-m", module, "--ingestion-id", run_id],
                                    env=command_env, capture_output=True, text=True, timeout=180)
            self.assertEqual(result.returncode, expected, result.stderr)
            if expected == 0:
                self.assertTrue("completed" in result.stdout or "consumer_already_complete" in result.stdout)
            if expected == 1:
                self.assertIn("consumer_failed", result.stderr)
                self.assertNotIn("consumer_completed", result.stdout)
            return result

        def row(run_id):
            db.rollback()
            return db.execute("SELECT * FROM meta.pipeline_run WHERE ingestion_id=%s", (run_id,)).fetchone()

        def load(table, run_id):
            db.rollback()
            return db.execute(sql.SQL("SELECT * FROM stg.{} WHERE consumer_run_id=%s ORDER BY change_kind").format(sql.Identifier(table)), (run_id,)).fetchall()

        try:
            seed = publish(1, [(2020, 10), (2021, None)])
            entries = {table: {"state":"pending", "plan":None} for table in raw.TABLES}
            db.execute("""UPDATE meta.pipeline_run SET stage_status='pending', stage_ingested_at='2025-01-02T00:00:00Z',
                       stage_snapshot_ids=%s WHERE ingestion_id=%s""", (Jsonb(entries), seed))
            db.commit()
            run("world_bank_stage", seed)
            run("world_bank_consumer", seed)
            self.assertEqual(sum(len(load(table, seed)) for table in raw.TABLES), 5)
            seed_observations = load("observation", seed)
            self.assertTrue(all(r["change_kind"] == "append" and r["ingestion_id"] == seed for r in seed_observations))
            self.assertIsNone(next(r for r in seed_observations if r["year"] == 2021)["value"])
            self.assertEqual(row(seed)["consumer_status"], "complete")

            exact = Decimal("12345678901234567890.123456789012345678")
            extract = publish(2, [(2020, exact)], country="Country earlier")
            earlier = reference(extract, "replay")
            run("world_bank_stage", earlier, 75, "country")
            run("world_bank_consumer", earlier, 1)
            self.assertEqual(sum(len(load(table, earlier)) for table in raw.TABLES), 0)
            later = publish(3, [(2020, 30)], country="Country later")
            run("world_bank_stage", later)
            run("world_bank_stage", earlier)
            earlier_manifest = row(earlier)["stage_snapshot_ids"]
            self.assertTrue(earlier_manifest["observation"]["overwrite"] and earlier_manifest["country"]["overwrite"])
            run("world_bank_consumer", earlier)
            observations = load("observation", earlier)
            self.assertEqual(len(observations), 2)
            opened, closed = observations
            self.assertEqual((opened["change_kind"], closed["change_kind"]), ("append", "close"))
            self.assertEqual(opened["value"], exact)
            self.assertEqual(opened["ingestion_id"], extract)
            self.assertEqual(opened["stage_run_id"], earlier)
            self.assertEqual(opened["consumer_run_id"], earlier)
            self.assertEqual(opened["version"], 2)
            self.assertEqual(closed["ingestion_id"], seed)
            self.assertEqual(closed["stage_run_id"], seed)
            self.assertEqual(closed["version"], 1)
            self.assertEqual(closed["valid_to"], opened["valid_from"])
            self.assertNotEqual(closed["stage_ingested_at"].date(), opened["stage_ingested_at"].date())
            self.assertEqual(opened["snapshot_id"], int(earlier_manifest["observation"]["append"]))
            self.assertEqual(closed["snapshot_id"], int(earlier_manifest["observation"]["overwrite"]))
            self.assertEqual(len(load("country", earlier)), 2)
            self.assertEqual(load("indicator", earlier), [])
            self.assertEqual(load("income_level", earlier), [])
            run("world_bank_consumer", later)
            self.assertEqual(next(r for r in load("observation", later) if r["change_kind"] == "append")["value"], 30)
            seed_before = load("observation", seed)
            later_before = load("observation", later)
            before_country = load("country", earlier)
            damaged = json.loads(json.dumps(earlier_manifest))
            damaged["observation"]["append"] = "9223372036854775807"
            db.execute("UPDATE meta.pipeline_run SET stage_snapshot_ids=%s WHERE ingestion_id=%s", (Jsonb(damaged), earlier))
            db.execute("""CREATE FUNCTION stg.fixture_guard() RETURNS trigger LANGUAGE plpgsql AS $$
                       BEGIN RAISE EXCEPTION 'completed retry attempted delete'; END $$""")
            db.execute("CREATE TRIGGER fixture_guard BEFORE DELETE ON stg.country FOR EACH STATEMENT EXECUTE FUNCTION stg.fixture_guard()")
            db.commit()
            prior_events = logs.filter_log_events(logGroupName=GROUP, logStreamNames=[earlier])["events"]
            prior_failures = sum(json.loads(event["message"])["event"] == "consumer_failed" for event in prior_events)
            completed_retry = run("world_bank_consumer", earlier)
            self.assertIn("consumer_already_complete", completed_retry.stdout)
            self.assertNotIn("consumer_failed", completed_retry.stderr)
            after_events = logs.filter_log_events(logGroupName=GROUP, logStreamNames=[earlier])["events"]
            self.assertEqual(sum(json.loads(event["message"])["event"] == "consumer_failed" for event in after_events), prior_failures)
            self.assertTrue(any(json.loads(event["message"])["event"] == "consumer_already_complete" for event in after_events))
            self.assertEqual(row(earlier)["consumer_status"], "complete")
            self.assertEqual(load("observation", earlier), observations)
            self.assertEqual(load("country", earlier), before_country)
            self.assertEqual(load("observation", seed), seed_before)
            self.assertEqual(load("observation", later), later_before)
            db.execute("DROP TRIGGER fixture_guard ON stg.country")
            db.execute("DROP FUNCTION stg.fixture_guard()")
            db.execute("UPDATE meta.pipeline_run SET stage_snapshot_ids=%s WHERE ingestion_id=%s", (Jsonb(earlier_manifest), earlier))
            db.commit()
            reprocess = reference(earlier, "reprocess_commit")
            original_stage_fields = (row(reprocess)["stage_status"], row(reprocess)["stage_snapshot_ids"])
            run("world_bank_consumer", reprocess)
            copied = load("observation", reprocess)
            self.assertEqual(len(copied), 2)
            self.assertEqual(next(r for r in copied if r["change_kind"] == "append")["value"], exact)
            self.assertTrue(all(r["consumer_run_id"] == reprocess for r in copied))
            self.assertEqual((row(reprocess)["stage_status"], row(reprocess)["stage_snapshot_ids"]), original_stage_fields)
            self.assertEqual(load("observation", earlier), observations)

            confirm = publish(4, [(2020, 30)], country="Country later")
            run("world_bank_stage", confirm)
            run("world_bank_consumer", confirm)
            self.assertEqual(row(confirm)["consumer_status"], "complete")
            self.assertTrue(all(not load(table, confirm) for table in raw.TABLES))

            retry = reference(earlier, "reprocess_commit")
            db.execute("""CREATE FUNCTION stg.fixture_reject() RETURNS trigger LANGUAGE plpgsql AS $$
                       BEGIN IF NEW.consumer_run_id = TG_ARGV[0] THEN RAISE EXCEPTION 'fixture'; END IF;
                       RETURN NEW; END $$""")
            db.execute(sql.SQL("CREATE TRIGGER fixture_reject BEFORE INSERT ON stg.country FOR EACH ROW EXECUTE FUNCTION stg.fixture_reject({})").format(sql.Literal(retry)))
            db.commit()
            run("world_bank_consumer", retry, 1)
            self.assertEqual(row(retry)["consumer_status"], "failed")
            self.assertTrue(all(not load(table, retry) for table in raw.TABLES))
            self.assertEqual(load("observation", earlier), observations)
            self.assertEqual(load("country", earlier), before_country)
            db.execute("DROP TRIGGER fixture_reject ON stg.country")
            db.execute("DROP FUNCTION stg.fixture_reject()")
            db.commit()
            run("world_bank_consumer", retry)
            retried = load("observation", retry)
            self.assertEqual(len(retried), len(observations))
            self.assertEqual(next(r for r in retried if r["change_kind"] == "append")["value"], exact)
            self.assertEqual(row(retry)["consumer_status"], "complete")
            self.assertEqual(load("observation", earlier), observations)

            bad = reference(earlier, "reprocess_commit")
            damaged = json.loads(json.dumps(earlier_manifest))
            damaged["observation"]["append"] = "9223372036854775807"
            db.execute("UPDATE meta.pipeline_run SET stage_snapshot_ids=%s WHERE ingestion_id=%s", (Jsonb(damaged), earlier))
            db.commit()
            run("world_bank_consumer", bad, 1)
            self.assertTrue(all(not load(table, bad) for table in raw.TABLES))
            self.assertEqual(load("observation", earlier), observations)
            db.execute("UPDATE meta.pipeline_run SET stage_snapshot_ids=%s WHERE ingestion_id=%s", (Jsonb(earlier_manifest), earlier))
            db.commit()
            events = logs.filter_log_events(logGroupName=GROUP, logStreamNames=[bad])["events"]
            messages = [json.loads(event["message"]) for event in events]
            self.assertTrue(any(m["event"] == "consumer_failed" for m in messages))
            self.assertTrue(any(m["event"] == "alert_published" and m["message_id"] for m in messages))
        finally:
            db.close()
            for table in raw.TABLES:
                try:
                    catalog.rest("DELETE", catalog.path(table) + "?purgeRequested=true")
                except Exception:
                    pass
            try:
                catalog.rest("DELETE", f"{catalog.root}/namespaces/{ns}")
            except Exception:
                pass
            for table in raw.TABLES:
                prefix = f"world-bank/{table}/date=2025-01-01/"
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=raw.BUCKET, Prefix=prefix):
                    for item in page.get("Contents", []):
                        if any(f"/ingestion_id={identifier}/" in item["Key"] for identifier in ids):
                            s3.delete_object(Bucket=raw.BUCKET, Key=item["Key"])
            for group in (GROUP, "/world-bank/lakehouse-stage-world-bank"):
                for identifier in ids:
                    try:
                        logs.delete_log_stream(logGroupName=group, logStreamName=identifier)
                    except Exception:
                        pass
            admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(ns)))
            admin.close()


if __name__ == "__main__":
    unittest.main()
