import argparse
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
import io
import json
import os
import unittest
from unittest.mock import Mock, patch

from world_bank_curated import __main__ as curated
from world_bank_curated.services import database


def artifacts():
    manifest = {"nodes": {}}
    catalog = {"nodes": {}, "errors": []}
    for model in curated.MODELS:
        key = "model.world_bank_curated." + model
        manifest["nodes"][key] = {"columns":{"id":{"description":"A key"}}, "depends_on":{"nodes":[]}}
        catalog["nodes"][key] = {"metadata":{"comment":"A model"}, "columns":{"id":{"type":"bigint", "comment":"A key"}}}
    manifest["nodes"]["model.world_bank_curated.fact_indicator_year"]["depends_on"]["nodes"] = [
        "source.world_bank_curated.stg.observation",
        "model.world_bank_curated.dim_country", "model.world_bank_curated.dim_indicator", "model.world_bank_curated.dim_year"]
    return manifest, catalog


def run_row(mode="extract"):
    return dict(ingestion_id="source", mode=mode, consumer_status="complete", curated_status="pending",
                stage_status="complete", stage_ingested_at=datetime(2025, 1, 1, tzinfo=timezone.utc))


class CuratedTests(unittest.TestCase):
    def test_gate_and_reprocess_date(self):
        runs = Mock()
        runs.get.return_value = run_row()
        invocation = run_row("reprocess_commit")
        invocation.update(ingestion_id="reprocess", source_ingestion_id="source")
        self.assertEqual(curated.select_stage(runs, invocation)["stage_ingested_at"].date().isoformat(), "2025-01-01")
        runs.get.assert_called_once_with("source")
        with self.assertRaises(ValueError):
            curated.select_stage(runs, dict(invocation, consumer_status="pending"))
        with self.assertRaises(ValueError):
            curated.select_stage(runs, dict(invocation, source_ingestion_id="reprocess"))
        runs.get.return_value = dict(run_row(), stage_status="failed")
        with self.assertRaises(ValueError):
            curated.select_stage(runs, invocation)

    def test_artifact_validation(self):
        manifest, catalog = artifacts()
        curated.validate_artifacts(json.dumps(manifest).encode(), json.dumps(catalog).encode())
        catalog["nodes"]["model.world_bank_curated.dim_country"]["columns"] = {}
        with self.assertRaises(ValueError):
            curated.validate_artifacts(json.dumps(manifest).encode(), json.dumps(catalog).encode())
        manifest, catalog = artifacts()
        manifest["nodes"]["model.world_bank_curated.fact_indicator_year"]["depends_on"]["nodes"] = []
        with self.assertRaises(ValueError):
            curated.validate_artifacts(json.dumps(manifest).encode(), json.dumps(catalog).encode())

    def test_second_upload_failure_marks_failed(self):
        runs = Mock()
        runs.get.return_value = run_row()
        reporter = Mock()
        s3 = Mock()
        s3.put_object.side_effect = [{}, OSError("second upload failed")]
        with patch.object(curated, "database", return_value="warehouse"), \
             patch.object(curated, "clients", return_value=(s3, Mock(), Mock())), \
             patch.object(curated, "Reporter", return_value=reporter), \
             patch.object(curated, "Runs", return_value=runs), \
             patch.object(curated, "build_artifacts", return_value=(b"{}", b"{}")), \
             patch.dict(os.environ, {"WORLD_BANK_ALERT_TOPIC_ARN":"local"}), \
             redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(curated.process(argparse.Namespace(ingestion_id="source")), 1)
        self.assertEqual([call.args[0] for call in runs.status.call_args_list], ["pending", "failed"])
        self.assertEqual(s3.put_object.call_count, 2)
        self.assertIn("date=2025-01-01/ingestion_id=source", s3.put_object.call_args_list[0].kwargs["Key"])
        reporter.failure.assert_called_once()

    def test_completed_run_skips_build(self):
        runs = Mock()
        runs.get.return_value = dict(run_row(), curated_status="complete")
        reporter = Mock()
        reporter.event.return_value = {"event":"curated_already_complete"}
        with patch.object(curated, "database", return_value="warehouse"), \
             patch.object(curated, "clients", return_value=(Mock(), Mock(), Mock())), \
             patch.object(curated, "Reporter", return_value=reporter), \
             patch.object(curated, "Runs", return_value=runs), \
             patch.object(curated, "build_artifacts") as build, \
             patch.dict(os.environ, {"WORLD_BANK_ALERT_TOPIC_ARN":"local"}), redirect_stdout(io.StringIO()):
            self.assertEqual(curated.process(argparse.Namespace(ingestion_id="source")), 0)
        build.assert_not_called()
        runs.status.assert_not_called()

    def test_test_database_guard(self):
        with patch.dict(os.environ, {"WORLD_BANK_CURATED_TEST_DATABASE":"warehouse"}):
            with self.assertRaises(ValueError):
                database()


if __name__ == "__main__":
    unittest.main()
