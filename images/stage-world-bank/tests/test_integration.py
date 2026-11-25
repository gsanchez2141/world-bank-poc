import json
from decimal import Decimal
import os
from pathlib import Path
import subprocess
import sys
import unittest
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from world_bank_ingest import raw
from world_bank_ingest.services import clients
from world_bank_stage.iceberg import Catalog
from world_bank_stage.services import GROUP


class StageIntegration(unittest.TestCase):
    def test_versions_replay_recovery_and_corruption(self):
        namespace = "stage_test_" + uuid4().hex
        admin = psycopg.connect(host="postgres", dbname="postgres", user="postgres", password="postgres", autocommit=True)
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(namespace)))
        db = psycopg.connect(host="postgres", dbname=namespace, user="postgres", password="postgres", row_factory=dict_row)
        db.execute(Path("world_bank_ingest/pipeline_run.sql").read_text())
        db.commit()
        s3, logs, _ = clients()
        catalog = Catalog(namespace)
        identifiers = []
        base_env = dict(os.environ, WORLD_BANK_STAGE_TEST_NAMESPACE=namespace)

        def publish(label, second, observations, country="Brazil"):
            run_id = "stage_" + uuid4().hex
            identifiers.append(run_id)
            when = f"2025-01-01T00:00:{second:02d}Z"
            original_clock = raw.now
            raw.now = lambda: when
            try:
                tables = {
                    "observation": [{"countryiso3code": "BRA", "indicator": {"id": "SP.POP.TOTL"},
                                     "date": str(year), "value": value} for year, value in observations],
                    "country": [{"id": "BRA", "name": country, "incomeLevel": {"id": "UMC"}}],
                    "indicator": [{"id": "SP.POP.TOTL", "name": "Population, total"}],
                    "income_level": [{"id": "UMC", "value": "Upper middle income"}],
                }
                for table, values in tables.items():
                    writer = raw.TableWriter(s3, table, "2025-01-01", run_id, 1)
                    body = json.dumps([{"page": 1, "pages": 1}, values], separators=(",", ":")).encode()
                    writer.page(1, 1, body, values, "https://fixture.example")
                    writer.publish()
            finally:
                raw.now = original_clock
            db.execute("""INSERT INTO meta.pipeline_run
                (ingestion_id,mode,request_countries,request_indicators,start_year,end_year,
                 raw_status,raw_date,successful_attempt,raw_attempt_count,raw_ingested_at)
                VALUES (%s,'extract',%s,%s,2020,2021,'complete','2025-01-01',1,1,%s)""",
                (run_id, ["BRA"], ["SP.POP.TOTL"], when))
            db.commit()
            return run_id

        def replay(source_id):
            run_id = "stage_" + uuid4().hex
            identifiers.append(run_id)
            db.execute("""INSERT INTO meta.pipeline_run
                (ingestion_id,mode,source_ingestion_id,request_countries,request_indicators,start_year,end_year,
                 raw_status,raw_date,successful_attempt,raw_ingested_at)
                SELECT %s,'replay',ingestion_id,request_countries,request_indicators,start_year,end_year,
                       raw_status,raw_date,successful_attempt,raw_ingested_at
                FROM meta.pipeline_run WHERE ingestion_id=%s""", (run_id, source_id))
            db.commit()
            return run_id

        def run(identifier, exit_code=0, inject=None):
            env = dict(base_env)
            if inject:
                env["WORLD_BANK_STAGE_TEST_EXIT_AFTER_COMMIT"] = inject
            result = subprocess.run([sys.executable, "-m", "world_bank_stage", "--ingestion-id", identifier],
                                    env=env, capture_output=True, text=True, timeout=180)
            self.assertEqual(result.returncode, exit_code, result.stderr)
            return result

        def state(identifier):
            db.rollback()
            return db.execute("SELECT * FROM meta.pipeline_run WHERE ingestion_id=%s", (identifier,)).fetchone()

        def current(table):
            return [r for r in catalog.rows(table) if r["is_current"]]

        def stage_objects():
            prefix = f"stage-tests/{namespace}/"
            return {obj["Key"] for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket="lakehouse-stage", Prefix=prefix) for obj in page.get("Contents", [])}

        try:
            seed = publish("seed", 1, [(2020, 10), (2021, 20)])
            run(seed)
            seed_state = state(seed)
            self.assertEqual(seed_state["stage_changed_count"], 5)
            self.assertTrue(all(v["append"] and v["overwrite"] is None for v in seed_state["stage_snapshot_ids"].values()))
            before = {table: catalog.inspect(table)["metadata"]["current-snapshot-id"] for table in raw.TABLES}
            before_objects = stage_objects()
            for table in raw.TABLES:
                metadata = catalog.inspect(table)["metadata"]
                self.assertEqual(metadata["location"], catalog.location(table))
                spec = next(item for item in metadata["partition-specs"] if item["spec-id"] == metadata["default-spec-id"])
                self.assertEqual(spec["fields"][0]["transform"], "day")
                self.assertTrue(all(row["ingestion_id"] == seed and row["version"] == 1 and
                                    row["valid_to"] is None for row in current(table)))
            older = publish("older", 2, [(2020, 11), (2021, 20)])
            confirmation = publish("confirmation", 3, [(2020, 10), (2021, 20)])
            run(confirmation)
            self.assertEqual(state(confirmation)["stage_changed_count"], 0)
            self.assertEqual(stage_objects(), before_objects)
            run(seed)
            replay_id = replay(older)
            run(replay_id)
            self.assertEqual(state(replay_id)["stage_changed_count"], 0)
            self.assertEqual(before, {table: catalog.inspect(table)["metadata"]["current-snapshot-id"] for table in raw.TABLES})
            null_run = publish("null", 4, [(2020, None)])
            run(null_run)
            null_state = state(null_run)["stage_snapshot_ids"]["observation"]
            self.assertTrue(null_state["overwrite"] and null_state["append"])
            overwritten = catalog.rows("observation", null_state["overwrite"])
            closed = next(r for r in overwritten if r["year"] == 2020 and r["version"] == 1)
            self.assertFalse(closed["is_current"])
            self.assertIsNotNone(closed["valid_to"])
            self.assertEqual(closed["ingestion_id"], seed)
            appended = catalog.rows("observation", null_state["append"])
            opened = next(r for r in appended if r["year"] == 2020 and r["is_current"])
            self.assertEqual(opened["version"], 2)
            self.assertEqual(opened["valid_from"], closed["valid_to"])
            self.assertEqual(opened["ingestion_id"], null_run)
            rows = current("observation")
            self.assertEqual({r["year"]: None if r["value"] is None else Decimal(r["value"]) for r in rows}, {2020: None, 2021: 20})
            target = publish("target", 5, [(2020, 25)], country="Country old")
            run(target, 75, "observation")
            pending = state(target)["stage_snapshot_ids"]["observation"]
            self.assertEqual(pending["state"], "pending")
            self.assertTrue(pending["plan"]["appends"])
            target_head = str(catalog.inspect("observation")["metadata"]["current-snapshot-id"])
            later = publish("later", 6, [(2020, 30)], country="Country new")
            run(later)
            later_head = str(catalog.inspect("observation")["metadata"]["current-snapshot-id"])
            run(target)
            recovered = state(target)["stage_snapshot_ids"]["observation"]
            self.assertEqual(recovered["append"], target_head)
            self.assertEqual(str(catalog.inspect("observation")["metadata"]["current-snapshot-id"]), later_head)
            self.assertEqual(Decimal(next(r for r in current("observation") if r["year"] == 2020)["value"]), Decimal(30))
            self.assertEqual(current("country")[0]["country_name"], "Country new")
            historical = catalog.rows("observation", target_head)
            self.assertEqual(Decimal(next(r for r in historical if r["year"] == 2020 and r["is_current"])["value"]), Decimal(25))
            run(target)
            corrupt = publish("corrupt", 7, [(2020, 31)])
            response_key = raw.base("observation", "2025-01-01", corrupt) + "attempt=1/responses/request_001_page_001.json"
            raw.put(s3, response_key, b"corrupt")
            failed = run(corrupt, 1)
            self.assertIn("stage_failed", failed.stderr)
            self.assertEqual(state(corrupt)["stage_status"], "failed")
            self.assertEqual(str(catalog.inspect("observation")["metadata"]["current-snapshot-id"]), later_head)
            events = logs.filter_log_events(logGroupName=GROUP, logStreamNames=[corrupt])["events"]
            messages = [json.loads(event["message"]) for event in events]
            self.assertTrue(any(m["event"] == "stage_failed" for m in messages))
            self.assertTrue(any(m["event"] == "alert_published" and m["message_id"] for m in messages))

            # Insert a closed version that overlaps the current 2020 version while
            # retaining exactly one current row. No valid writer produces this.
            relation = f"ice.{namespace}.observation"
            catalog.query(f'''INSERT INTO {relation}
                SELECT * REPLACE (
                    99 AS version,
                    valid_from - INTERVAL '1 day' AS valid_from,
                    valid_from + INTERVAL '1 day' AS valid_to,
                    false AS is_current
                )
                FROM {relation} WHERE year=2020 AND is_current=true''', expect_rows=False)
            corrupt_head = str(catalog.inspect("observation")["metadata"]["current-snapshot-id"])
            corrupt_objects = stage_objects()
            history_run = publish("invalid_history", 8, [(2020, 32)])
            history_failure = run(history_run, 1)
            self.assertIn("stage_failed", history_failure.stderr)
            self.assertEqual(state(history_run)["stage_status"], "failed")
            self.assertEqual(str(catalog.inspect("observation")["metadata"]["current-snapshot-id"]), corrupt_head)
            self.assertEqual(stage_objects(), corrupt_objects)
            history_events = logs.filter_log_events(logGroupName=GROUP, logStreamNames=[history_run])["events"]
            history_messages = [json.loads(event["message"]) for event in history_events]
            self.assertTrue(any(message["event"] == "stage_failed" for message in history_messages))
            self.assertTrue(any(message["event"] == "alert_published" and message["message_id"] for message in history_messages))

        finally:
            db.close()
            for table in raw.TABLES:
                try:
                    catalog.rest("DELETE", catalog.path(table) + "?purgeRequested=true")
                except Exception:
                    pass
            try:
                catalog.rest("DELETE", f"{catalog.root}/namespaces/{namespace}")
            except Exception:
                pass
            for table in raw.TABLES:
                prefix = f"world-bank/{table}/date=2025-01-01/"
                paginator = s3.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=raw.BUCKET, Prefix=prefix):
                    for item in page.get("Contents", []):
                        if any(f"/ingestion_id={identifier}/" in item["Key"] for identifier in identifiers):
                            s3.delete_object(Bucket=raw.BUCKET, Key=item["Key"])
            for identifier in identifiers:
                try:
                    logs.delete_log_stream(logGroupName=GROUP, logStreamName=identifier)
                except Exception:
                    pass
            admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(namespace)))
            admin.close()


if __name__ == "__main__":
    unittest.main()
