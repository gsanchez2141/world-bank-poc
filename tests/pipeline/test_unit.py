"""Offline validation of the Airflow trigger and immutable run identity."""
import unittest
from contextlib import redirect_stdout
import io
from types import SimpleNamespace
import importlib.util
from unittest.mock import Mock, patch
from world_bank_pipeline_support import gate, runtime

CONFIG = {'countries':['BRA','CAN'], 'indicators':['SP.POP.TOTL','NY.GDP.MKTP.CD'],
          'start_year':2018, 'end_year':2024}

class GateValidation(unittest.TestCase):
    def test_gate_rejects_non_object_before_connecting(self):
        for conf in ([], False, 0, "", None):
            with self.subTest(conf=conf), patch.object(gate, "connect") as connect:
                with self.assertRaises(ValueError):
                    gate.gate(dag_run=SimpleNamespace(conf=conf, run_id="run"))
                connect.assert_not_called()

    def test_scope_normalization_and_resume(self):
        conf = {'ingestion_id':'run1','countries':['CAN','BRA'], 'start_year':2020}
        mode, source, scope = gate.validate_conf(conf, CONFIG)
        self.assertEqual((mode, source), ('extract', None))
        self.assertEqual(scope['countries'], ['BRA','CAN'])
        existing = {'mode': mode, 'source_ingestion_id':None, 'abandoned_at':None,
                    'request_countries':scope['countries'], 'request_indicators':scope['indicators'],
                    'start_year':scope['start_year'], 'end_year':scope['end_year']}
        self.assertEqual(gate.validate_conf({'ingestion_id':'run1'}, CONFIG, existing), (mode, source, scope))
        with self.assertRaises(ValueError):
            gate.validate_conf({'ingestion_id':'run1','countries':['BRA']}, CONFIG, existing)
        with self.assertRaises(ValueError):
            gate.validate_conf({'ingestion_id':'run1','abandon_previous':False}, CONFIG, existing)
        self.assertEqual(gate.validate_conf({'abandon_previous':False}, CONFIG)[0], 'extract')
        existing['abandoned_at'] = '2025-01-01'
        with self.assertRaises(ValueError):
            gate.validate_conf({'ingestion_id':'run1'}, CONFIG, existing)

    def test_invalid_inputs_fail(self):
        for conf in ({'unknown':1}, {'countries':['BRA','BRA']},
                     {'start_year':True}, {'abandon_previous':'yes'}, {'mode':'replay'}):
            with self.subTest(conf=conf), self.assertRaises(ValueError):
                gate.validate_conf(conf, CONFIG)
        for value in ('/bad', '', 'a' * 129):
            with self.assertRaises(ValueError):
                gate.ingestion_id({'ingestion_id':value}, 'manual')
        self.assertEqual(gate.fallback_id('manual__123'), gate.fallback_id('manual__123'))

    def test_rebuild_scope_and_immutable_anchor(self):
        mode, source, scope = gate.validate_conf({'ingestion_id':'restore','mode':'rebuild'}, CONFIG)
        self.assertEqual((mode, source), ('rebuild', None))
        self.assertTrue(all(value is None for value in scope.values()))
        for override in ({'countries':['BRA']}, {'indicators':['SP.POP.TOTL']},
                         {'start_year':2020}, {'end_year':2021}, {'abandon_previous':False}):
            with self.subTest(override=override), self.assertRaises(ValueError):
                gate.validate_conf(dict(mode='rebuild', ingestion_id='restore', **override), CONFIG)
        source_row = {'mode':'extract', 'stage_status':'complete'}
        mode, source, _ = gate.validate_conf({'ingestion_id':'restore','mode':'rebuild',
                                              'source_ingestion_id':'anchor'}, CONFIG, source=source_row)
        self.assertEqual(source, 'anchor')
        existing = {'mode':'rebuild','source_ingestion_id':'anchor','abandoned_at':None,
                    'request_countries':None,'request_indicators':None,'start_year':None,'end_year':None}
        with self.assertRaises(ValueError):
            gate.validate_conf({'ingestion_id':'restore','source_ingestion_id':'different'}, CONFIG,
                               existing=existing, source=source_row)

    def test_failure_identity_fallback(self):
        from types import SimpleNamespace
        run = SimpleNamespace(conf={'ingestion_id':'valid'}, run_id='airflow-run')
        self.assertEqual(runtime.failure_id({'dag_run':run}), 'valid')
        run.conf = {'ingestion_id':'not valid'}
        self.assertEqual(runtime.failure_id({'dag_run':run}), gate.fallback_id('airflow-run'))

    def test_replay_inherits_completed_source(self):
        source = {'mode':'extract', 'raw_status':'complete', 'stage_status':'complete',
                  'request_countries':['BRA'], 'request_indicators':['SP.POP.TOTL'],
                  'start_year':2020, 'end_year':2021}
        mode, identifier, scope = gate.validate_conf({'mode':'replay','source_ingestion_id':'source'}, CONFIG, source=source)
        self.assertEqual((mode, identifier, scope['start_year']), ('replay','source',2020))
        with self.assertRaises(ValueError):
            gate.validate_conf({'mode':'replay','source_ingestion_id':'source','abandon_previous':False}, CONFIG, source=source)
        with self.assertRaises(ValueError):
            gate.validate_conf({'mode':'replay','source_ingestion_id':'source','countries':['BRA']}, CONFIG, source=source)
        source['raw_status'] = 'failed'
        with self.assertRaises(ValueError):
            gate.validate_conf({'mode':'replay','source_ingestion_id':'source'}, CONFIG, source=source)

class LocalClient(unittest.TestCase):
    def test_lost_post_response_reports_run_identity(self):
        spec = importlib.util.spec_from_file_location('run_pipeline', '/opt/world-bank/scripts/run-pipeline.py')
        client = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(client)
        opener = Mock()
        opener.open.side_effect = TimeoutError("response lost")
        output = io.StringIO()
        with patch.object(client, 'build_opener', return_value=opener), \
             patch.object(client, 'uuid4', return_value=SimpleNamespace(hex='abc123')), \
             redirect_stdout(output):
            with self.assertRaises(TimeoutError):
                client.trigger_and_wait('http://127.0.0.1:8080', {}, timeout=1)
        self.assertEqual(opener.open.call_count, 1)
        self.assertIn("airflow_run_id=manual_abc123", output.getvalue())
        self.assertIn("ingestion_id=" + client.fallback_id("manual_abc123"), output.getvalue())

    def test_local_url_and_terminal_states(self):
        spec = importlib.util.spec_from_file_location('run_pipeline', '/opt/world-bank/scripts/run-pipeline.py')
        client = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(client)
        self.assertEqual(client.validate_url('http://127.0.0.1:8080/'), 'http://127.0.0.1:8080')
        for url in ('http://localhost:8080', 'https://127.0.0.1:8080', 'http://example.com:80'):
            with self.assertRaises(ValueError):
                client.validate_url(url)
        class Response:
            def __init__(self, data): self.data = data
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self): return self.data
        class Opener:
            def __init__(self, state): self.state, self.run_id = state, None
            def open(self, req, timeout):
                import json
                if req.get_method() == 'POST':
                    self.run_id = json.loads(req.data)['dag_run_id']
                    return Response(json.dumps({'dag_run_id':self.run_id}).encode())
                return Response(json.dumps({'dag_run_id':self.run_id, 'state':self.state}).encode())
        for state, result in (('success', 0), ('failed', 1)):
            with patch.object(client, 'build_opener', return_value=Opener(state)):
                self.assertEqual(client.trigger_and_wait('http://127.0.0.1:8080', {}, timeout=1, poll_interval=.01), result)
        with patch.object(client, 'build_opener', return_value=Opener('running')):
            self.assertEqual(client.trigger_and_wait('http://127.0.0.1:8080', {}, timeout=.02, poll_interval=.01), 1)



if __name__ == '__main__':
    unittest.main()
