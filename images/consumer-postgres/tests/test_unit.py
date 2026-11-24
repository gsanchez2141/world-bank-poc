from datetime import datetime, timezone
from contextlib import redirect_stdout
import argparse
import io
import os
from decimal import Decimal
import unittest
from unittest.mock import Mock, patch

from world_bank_consumer import __main__ as cli, snapshots
from world_bank_consumer.__main__ import select_source
from world_bank_stage.iceberg import fields


def source(count=0, append=None, overwrite=None):
    entries = {name: dict(state="complete", append=None, overwrite=None, changed_count=0)
               for name in snapshots.TABLES}
    entries["observation"] = dict(state="complete", append=append, overwrite=overwrite, changed_count=count)
    return dict(mode="extract", stage_status="complete", ingestion_id="stage-run",
                stage_ingested_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
                stage_changed_count=count, stage_snapshot_ids=entries)


def stage_row(version, value, closed=False):
    row = {name: "text" for name, _ in fields("observation")}
    row.update(country_iso3="BRA", indicator_code="SP.POP.TOTL", year=2020, value=value,
               raw_record_index=0, api_page=1, version=version, is_current=not closed,
               raw_ingested_at="2025-01-01T00:00:00+00:00",
               stage_ingested_at="2025-01-01T00:00:00+00:00" if closed else "2025-01-02T00:00:00+00:00",
               valid_from="2025-01-01T00:00:00+00:00" if closed else "2025-01-02T00:00:00+00:00",
               valid_to="2025-01-02T00:00:00+00:00" if closed else None,
               stage_run_id="seed" if closed else "stage-run")
    return row


class FakeCatalog:
    def __init__(self, opened, closed=None, parent=10):
        self.opened, self.closed, self.parent = opened, closed or [], parent

    def location(self, table):
        return "s3://lakehouse-stage/world-bank/" + table

    def inspect(self, table):
        return {"metadata-location": self.location(table) + "/metadata/current.json", "metadata": {"snapshots": [
            {"snapshot-id": 10, "summary":{"operation":"overwrite"}},
            {"snapshot-id": 11, "parent-snapshot-id":self.parent, "summary":{"operation":"append"}}]}}

    def query(self, sql):
        return [dict(row) for row in (self.opened if "snapshot_from_id = 11" in sql else self.closed)]


class ConsumerUnit(unittest.TestCase):
    def test_completed_run_does_not_select_or_replace(self):
        runs = Mock()
        runs.get.return_value = {"consumer_status": "complete"}
        reporter = Mock()
        reporter.event.return_value = {"event": "consumer_already_complete"}
        output = io.StringIO()
        with patch.object(cli, "namespace", return_value="world_bank"), \
             patch.object(cli, "clients", return_value=(Mock(), Mock(), Mock())), \
             patch.object(cli, "Reporter", return_value=reporter), \
             patch.object(cli, "ConsumerRuns", return_value=runs), \
             patch.object(cli, "select_source") as select, \
             patch.object(cli, "read_changes") as read, \
             patch.dict(os.environ, {"WORLD_BANK_ALERT_TOPIC_ARN": "local"}), \
             redirect_stdout(output):
            self.assertEqual(cli.process(argparse.Namespace(ingestion_id="complete")), 0)
        runs.get.assert_called_once_with("complete")
        runs.replace.assert_not_called()
        runs.fail.assert_not_called()
        runs.close.assert_called_once()
        select.assert_not_called()
        read.assert_not_called()
        self.assertIn("consumer_already_complete", output.getvalue())

    def test_zero_skips_catalog(self):
        factory = Mock()
        self.assertEqual(snapshots.read_changes(source(), factory), {name: [] for name in snapshots.TABLES})
        factory.assert_not_called()

    def test_manifest_id_bounds(self):
        for bad in ("0", "01", "-1", str(2**63), 1, "1e3"):
            with self.assertRaises(ValueError):
                snapshots.validate_manifest(source(1, bad))
        snapshots.validate_manifest(source(1, str(2**63-1)))
        with self.assertRaises(ValueError):
            snapshots.validate_manifest(source(0, "11"))

    def test_pair_and_exact_decimal(self):
        commit = source(1, "11", "10")
        opened = stage_row(2, "12345678901234567890.123456789012345678")
        closed = stage_row(1, None, True)
        result = snapshots.read_changes(commit, lambda: FakeCatalog([opened], [closed]))
        self.assertEqual([r["change_kind"] for r in result["observation"]], ["close", "append"])
        self.assertEqual(result["observation"][1]["value"], Decimal("12345678901234567890.123456789012345678"))
        self.assertIsNone(result["observation"][0]["value"])
        self.assertEqual([r["snapshot_id"] for r in result["observation"]], [10, 11])
        with self.assertRaises(ValueError):
            snapshots.read_changes(commit, lambda: FakeCatalog([opened], []))
        with self.assertRaises(ValueError):
            snapshots.read_changes(commit, lambda: FakeCatalog([opened], [closed], parent=9))
        bad = dict(opened, raw_object_key=None)
        with self.assertRaises(ValueError):
            snapshots.read_changes(commit, lambda: FakeCatalog([bad], [closed]))

    def test_run_selection(self):
        runs = Mock()
        source_row = {"ingestion_id":"source","mode":"replay"}
        runs.get.return_value = source_row
        invocation = {"ingestion_id":"load","mode":"reprocess_commit","source_ingestion_id":"source"}
        self.assertIs(select_source(runs, invocation), source_row)
        with self.assertRaises(ValueError):
            select_source(runs, dict(invocation, source_ingestion_id="load"))
        with self.assertRaises(ValueError):
            select_source(runs, dict(invocation, mode="rebuild"))


if __name__ == "__main__":
    unittest.main()
