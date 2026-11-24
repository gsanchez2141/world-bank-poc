import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import unittest
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import psycopg

from world_bank_ingest import raw
from world_bank_ingest.services import clients, GROUP


class FixtureHandler(BaseHTTPRequestHandler):
    fail = True
    calls = []

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        self.calls.append((parsed.path, query))
        if query.get("per_page") != ["50"] or query.get("format") != ["json"]:
            self.send_error(400)
            return
        if "/country/BRA/indicator/SP.POP.TOTL" in parsed.path:
            if query.get("source") != ["2"] or query.get("date") != ["2020:2021"]:
                self.send_error(400)
                return
            page = int(query["page"][0])
            if page == 2 and self.fail:
                self.send_error(400)
                return
            body = (Path("tests/fixtures") / f"observation_page_{page:03}.json").read_bytes()
        elif parsed.path.endswith("/country/BRA"):
            body = b'[{"page":1,"pages":1},[{"id":"BRA","iso2Code":"BR","name":"Brazil","incomeLevel":{"id":"UMC","value":"Upper middle income"}}]]'
        elif parsed.path.endswith("/indicator/SP.POP.TOTL"):
            body = b'[{"page":1,"pages":1},[{"id":"SP.POP.TOTL","name":"Population"}]]'
        elif parsed.path.endswith("/incomelevel"):
            body = b'[{"page":1,"pages":1},[{"id":"UMC","value":"Upper middle income"}]]'
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class Integration(unittest.TestCase):
    def test_fail_retry_repeat_replay(self):
        s3, logs, _ = clients()
        db = psycopg.connect("host=postgres dbname=warehouse user=postgres password=postgres")
        ingestion_id = "test_" + uuid4().hex
        replay_id = "test_" + uuid4().hex
        server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        env = dict(os.environ, WORLD_BANK_TEST_API_URL=f"http://127.0.0.1:{server.server_port}/v2")
        command = [sys.executable, "-m", "world_bank_ingest", "--ingestion-id", ingestion_id,
                   "--countries", "BRA", "--indicators", "SP.POP.TOTL", "--start-year", "2020", "--end-year", "2021"]

        def invoke(cmd):
            return subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=120)

        def row(identifier):
            return db.execute("SELECT raw_status,successful_attempt,raw_attempt_count,raw_date FROM meta.pipeline_run WHERE ingestion_id=%s", (identifier,)).fetchone()

        try:
            first = invoke(command)
            self.assertEqual(first.returncode, 1, first.stderr)
            self.assertIn("ingest_failed", first.stderr)
            self.assertEqual(row(ingestion_id)[:3], ("failed", None, 1))
            raw_date = row(ingestion_id)[3]
            first_key = raw.base("observation", raw_date, ingestion_id) + "attempt=1/responses/request_001_page_001.json"
            first_body = s3.get_object(Bucket=raw.BUCKET, Key=first_key)["Body"].read()
            self.assertEqual(first_body, Path("tests/fixtures/observation_page_001.json").read_bytes())
            events = logs.filter_log_events(logGroupName=GROUP, logStreamNames=[ingestion_id])["events"]
            messages = [json.loads(event["message"]) for event in events]
            self.assertTrue(any(m["event"] == "ingest_failed" for m in messages))
            self.assertTrue(any(m["event"] == "alert_published" and m["message_id"] for m in messages))
            FixtureHandler.fail = False
            second = invoke(command)
            self.assertEqual(second.returncode, 0, second.stderr)
            self.assertEqual(row(ingestion_id)[:3], ("complete", 2, 2))
            raw.verify(s3, ingestion_id, raw_date, 2, True)
            self.assertEqual(s3.get_object(Bucket=raw.BUCKET, Key=first_key)["Body"].read(), first_body)
            manifest_key = raw.base("observation", raw_date, ingestion_id) + "manifest.json"
            manifest = json.loads(s3.get_object(Bucket=raw.BUCKET, Key=manifest_key)["Body"].read())
            records = json.loads(s3.get_object(Bucket=raw.BUCKET, Key=manifest["record_keys"][0])["Body"].read())
            self.assertIsNone(records[0]["value"])
            self.assertEqual(records[1]["value"], 42)
            for index, entry in enumerate(manifest["responses"], 1):
                body = s3.get_object(Bucket=raw.BUCKET, Key=entry["key"])["Body"].read()
                self.assertEqual(body, Path(f"tests/fixtures/observation_page_{index:03}.json").read_bytes())
                self.assertEqual(hashlib.sha256(body).hexdigest(), entry["sha256"])
                self.assertEqual(records[index-1]["raw_response_key"], entry["key"])
            call_count = len(FixtureHandler.calls)
            repeat = invoke(command)
            self.assertEqual(repeat.returncode, 0, repeat.stderr)
            self.assertEqual(len(FixtureHandler.calls), call_count)
            self.assertEqual(row(ingestion_id)[2], 2)
            replay = invoke([sys.executable, "-m", "world_bank_ingest", "--ingestion-id", replay_id,
                             "--mode", "replay", "--source-ingestion-id", ingestion_id])
            self.assertEqual(replay.returncode, 0, replay.stderr)
            self.assertEqual(len(FixtureHandler.calls), call_count)
            self.assertEqual(row(replay_id)[:2], ("complete", 2))
            for identifier in (ingestion_id, replay_id):
                events = logs.filter_log_events(logGroupName=GROUP, logStreamNames=[identifier])["events"]
                self.assertTrue(any(json.loads(event["message"])["event"] in ("ingest_completed", "ingest_already_complete") for event in events))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
            db.rollback()
            db.execute("DELETE FROM meta.pipeline_run WHERE ingestion_id IN (%s,%s)", (ingestion_id, replay_id))
            db.commit()
            db.close()
            for table in raw.TABLES:
                for identifier in (ingestion_id, replay_id):
                    prefix = f"world-bank/{table}/"
                    objects = s3.list_objects_v2(Bucket=raw.BUCKET, Prefix=prefix).get("Contents", [])
                    for obj in objects:
                        if f"/ingestion_id={identifier}/" in obj["Key"]:
                            s3.delete_object(Bucket=raw.BUCKET, Key=obj["Key"])
            for identifier in (ingestion_id, replay_id):
                try:
                    logs.delete_log_stream(logGroupName=GROUP, logStreamName=identifier)
                except Exception:
                    pass


if __name__ == "__main__":
    unittest.main()
