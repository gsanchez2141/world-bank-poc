"""Offline tests for deterministic trigger-and-wait behavior."""
from contextlib import contextmanager
import hashlib
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from world_bank_pipeline_support import backfill
from world_bank_pipeline_support.gate import fallback_id


class BackfillTests(unittest.TestCase):
    def setUp(self):
        self.parent = SimpleNamespace(run_id="parent-run", conf={"mode": "rebuild"})
        self.child_id = backfill.child_run_id(self.parent.run_id)
        self.conf = {"mode": "rebuild", "ingestion_id": fallback_id(self.child_id)}
        self.child = dict(dag_id=backfill.WRITER, run_id=self.child_id, conf=self.conf, state="success")

    def test_child_config_and_identity(self):
        self.assertEqual(backfill.child_run_id("parent-run"),
                         "manual__wbbackfill_" + hashlib.sha256(b"parent-run").hexdigest())
        self.assertEqual(backfill.child_run_id("parent-run"), self.child_id)
        self.assertNotEqual(backfill.child_run_id("other-run"), self.child_id)
        self.assertEqual(backfill.child_conf(self.parent.conf, self.child_id), self.conf)
        for mode in backfill.MODES:
            self.assertEqual(backfill.child_conf({"mode": mode, "ingestion_id": "chosen"}, self.child_id),
                             {"mode": mode, "ingestion_id": "chosen"})
        for bad in ([], {"ingestion_id":"bad id"}, {"mode":"unknown"}, {"extra":1},
                    {"source_ingestion_id":"bad/id"}):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                backfill.child_conf(bad, self.child_id)

    def test_exact_lookup_rejects_ambiguous(self):
        session = Mock()
        @contextmanager
        def sessions():
            yield session
        match = SimpleNamespace(dag_id=backfill.WRITER, run_id=self.child_id, conf=self.conf, state="success")
        with patch.object(backfill, "create_session", sessions), patch.object(backfill.DagRun, "find", return_value=[match]) as find:
            self.assertEqual(backfill.find_exact(self.child_id), self.child)
            find.assert_called_once_with(dag_id=backfill.WRITER, run_id=[self.child_id], session=session)
        with patch.object(backfill, "create_session", sessions), patch.object(backfill.DagRun, "find", return_value=[match, match]):
            with self.assertRaises(ValueError):
                backfill.find_exact(self.child_id)

    def test_existing_child_is_reattached_without_trigger(self):
        with patch.object(backfill, "find_exact", return_value=self.child) as find, \
             patch.object(backfill, "trigger_dag") as trigger:
            self.assertIsNone(backfill.trigger_and_wait(dag_run=self.parent))
            self.assertEqual(find.call_count, 2)
            trigger.assert_not_called()

    def test_new_child_and_duplicate_race(self):
        with patch.object(backfill, "find_exact", side_effect=[None, self.child, self.child]), \
             patch.object(backfill, "trigger_dag") as trigger:
            backfill.trigger_and_wait(dag_run=self.parent)
            trigger.assert_called_once_with(dag_id=backfill.WRITER, run_id=self.child_id,
                                            conf=self.conf, replace_microseconds=False)
        class Existing(Exception):
            pass
        with patch.object(backfill, "DagRunAlreadyExists", Existing), \
             patch.object(backfill, "find_exact", side_effect=[None, self.child, self.child]), \
             patch.object(backfill, "trigger_dag", side_effect=Existing):
            backfill.trigger_and_wait(dag_run=self.parent)

    def test_missing_conflicting_failed_and_timeout(self):
        for mismatch in (None, dict(self.child, run_id="wrong"), dict(self.child, conf={"mode":"extract"})):
            with self.subTest(mismatch=mismatch), patch.object(backfill, "find_exact", side_effect=[None, mismatch]), \
                 patch.object(backfill, "trigger_dag"):
                with self.assertRaises(ValueError):
                    backfill.trigger_and_wait(dag_run=self.parent)
        failed = dict(self.child, state="failed")
        with patch.object(backfill, "find_exact", return_value=failed):
            with self.assertRaises(ValueError):
                backfill.trigger_and_wait(dag_run=self.parent)
        running = dict(self.child, state="running")
        with patch.object(backfill, "find_exact", return_value=running), \
             patch.object(backfill.time, "monotonic", side_effect=[0, 1800]):
            with self.assertRaises(TimeoutError):
                backfill.trigger_and_wait(dag_run=self.parent)


if __name__ == "__main__":
    unittest.main()
