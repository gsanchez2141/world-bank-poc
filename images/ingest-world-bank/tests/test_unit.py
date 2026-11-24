import argparse
from contextlib import redirect_stderr
import io
import os
import hashlib
import unittest
from unittest.mock import Mock, patch

import requests
import simplejson as json

from world_bank_ingest import __main__ as cli, raw, source
from world_bank_ingest.services import clients


class Store:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.objects[Key] = Body


class IngestTests(unittest.TestCase):
    def test_scope(self):
        args = argparse.Namespace(countries=None, indicators=None, start_year=None, end_year=None)
        scope = source.load_scope(args)
        self.assertEqual(len(scope["countries"]) * len(scope["indicators"]) * (scope["end_year"] - scope["start_year"] + 1), 252)
        args.countries, args.indicators, args.start_year, args.end_year = "USA,BRA", "SP.POP.TOTL", 2020, 2021
        self.assertEqual(source.load_scope(args)["countries"], ["BRA", "USA"])
        args.countries = "BRA,BRA"
        with self.assertRaises(ValueError):
            source.load_scope(args)

    def test_batch_lineage_and_precision(self):
        store = Store()
        writer = raw.TableWriter(store, "observation", "2026-11-24", "run", 1)
        bodies = []
        for page, size in enumerate((37, 128, 5), 1):
            records = [{"value": None if i == 0 else json.Decimal("1.234567890123456789")} for i in range(size)]
            body = json.dumps([{"page": page, "pages": 3}, records], use_decimal=True).encode()
            bodies.append(body)
            writer.page(1, page, body, records, "http://127.0.0.1/page")
        writer.flush()
        batches = [json.loads(store.objects[key], use_decimal=True) for key in writer.manifest["record_keys"]]
        self.assertEqual([len(batch) for batch in batches], [100, 70])
        records = sum(batches, [])
        self.assertIsNone(records[0]["value"])
        self.assertEqual(records[1]["value"], json.Decimal("1.234567890123456789"))
        self.assertEqual([records[n]["raw_record_index"] for n in (0, 37, 165)], [0, 0, 0])
        for record in records:
            self.assertEqual(hashlib.sha256(store.objects[record["raw_response_key"]]).hexdigest(), record["response_sha256"])
            self.assertIn(record, json.loads(store.objects[record["raw_object_key"]], use_decimal=True))
        self.assertEqual([store.objects[x["key"]] for x in writer.manifest["responses"]], bodies)

    def test_page_validation(self):
        with self.assertRaises(ValueError):
            source.validate_page(b'{}', 1, None, "observation")
        with self.assertRaises(ValueError):
            source.validate_page(b'[{"page":2,"pages":2},[]]', 1, None, "observation")
        with self.assertRaises(ValueError):
            source.validate_page(b'[{"page":2,"pages":3},[]]', 2, 2, "observation")
        with self.assertRaises(ValueError):
            source.validate_page(b'[{"page":1,"pages":1},[{"value":"1"}]]', 1, None, "observation")
        with self.assertRaises(ValueError):
            source.validate_page(b'[{"page":1,"pages":1},[{}]]', 1, None, "observation")
        self.assertEqual(source.validate_page(b'[{"page":1,"pages":0,"total":0},null]', 1, None, "observation"), (0, []))

    def test_retry_and_terminal(self):
        ok = Mock(status_code=200, content=b"[]", url="http://test")
        ok.raise_for_status.return_value = None
        retry = Mock(status_code=429)
        session = Mock()
        session.get.side_effect = [retry, ok]
        sleeper = Mock()
        self.assertEqual(source.fetch(session, "http://test", {}, 1, Mock(), sleeper, lambda a, b: 0), (b"[]", "http://test"))
        self.assertEqual(session.get.call_count, 2)
        session.get.side_effect = requests.Timeout()
        with self.assertRaises(ValueError):
            source.fetch(session, "http://test", {}, 1, Mock(), sleeper, lambda a, b: 0)
        session.get.side_effect = None
        fail = Mock(status_code=400)
        fail.raise_for_status.side_effect = requests.HTTPError("400")
        session.get.return_value = fail
        with self.assertRaises(requests.HTTPError):
            source.fetch(session, "http://test", {}, 1, Mock(), sleeper, lambda a, b: 0)

    def test_stream_creation_failure_still_publishes_alert(self):
        logs, sns = Mock(), Mock()
        logs.create_log_stream.side_effect = RuntimeError("CloudWatch unavailable")
        logs.put_log_events.side_effect = RuntimeError("CloudWatch unavailable")
        sns.publish.return_value = {"MessageId": "local-message"}
        args = argparse.Namespace(ingestion_id="run-1", mode="extract", source_ingestion_id=None)
        stderr = io.StringIO()
        with patch.object(cli, "clients", return_value=(Mock(), logs, sns)), \
             patch.object(cli, "Runs") as runs, \
             patch.dict(os.environ, {"WORLD_BANK_ALERT_TOPIC_ARN": "local-topic"}), \
             redirect_stderr(stderr):
            result = cli.run(args, {})
        self.assertEqual(result, 1)
        self.assertEqual(json.loads(stderr.getvalue())["event"], "ingest_failed")
        runs.assert_not_called()
        sns.publish.assert_called_once()
        publication = sns.publish.call_args.kwargs
        self.assertEqual(publication["TopicArn"], "local-topic")
        message = json.loads(publication["Message"])
        self.assertEqual(message["ingestion_id"], "run-1")
        self.assertEqual(message["task_name"], "lakehouse-ingest-world-bank")
        self.assertIn("error", message)

    def test_local_only(self):
        with self.assertRaises(ValueError):
            source.api_base("http://example.com")
        with self.assertRaises(ValueError):
            clients()


if __name__ == "__main__":
    unittest.main()
