"""Offline validation of the Airflow trigger and immutable run identity."""
import unittest
import os
import runpy
from contextlib import redirect_stdout
import io
from types import SimpleNamespace
import importlib.util
from unittest.mock import Mock, patch
from world_bank_pipeline_support import gate, runtime

CONFIG = {'countries':['BRA','CAN'], 'indicators':['SP.POP.TOTL','NY.GDP.MKTP.CD'],
          'start_year':2018, 'end_year':2024}

class DagSchedule(unittest.TestCase):
    def test_creation_pause_follows_schedule(self):
        for value, expected_schedule, paused in (
            (None, None, False),
            ("", None, False),
            ("15 2 * * *", "15 2 * * *", True),
        ):
            with self.subTest(schedule=value):
                environment = {} if value is None else {"WORLD_BANK_PIPELINE_SCHEDULE": value}
                with patch.dict(os.environ, environment, clear=True):
                    dag = runpy.run_path("/opt/airflow/dags/world_bank_pipeline.py")["dag"]
                self.assertEqual(dag.schedule_interval, expected_schedule)
                self.assertEqual(dag.is_paused_upon_creation, paused)


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

    def test_backfill_uses_exact_dag_path_and_configuration(self):
        import json
        spec = importlib.util.spec_from_file_location('run_pipeline', '/opt/world-bank/scripts/run-pipeline.py')
        client = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(client)

        class Response:
            def __init__(self, data): self.data = data
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self): return json.dumps(self.data).encode()

        class Opener:
            def __init__(self): self.requests = []
            def open(self, req, timeout):
                self.requests.append(req)
                if req.get_method() == 'POST':
                    self.run_id = json.loads(req.data)['dag_run_id']
                    return Response({'dag_run_id': self.run_id})
                return Response({'dag_run_id': self.run_id, 'state': 'success'})

        opener = Opener()
        conf = {'ingestion_id': 'example', 'mode': 'extract'}
        with patch.object(client, 'build_opener', return_value=opener):
            self.assertEqual(client.trigger_and_wait('http://127.0.0.1:8080', conf,
                             dag='world_bank_backfill'), 0)
        prefix = 'http://127.0.0.1:8080/api/v1/dags/world_bank_backfill/dagRuns'
        self.assertEqual(opener.requests[0].full_url, prefix)
        self.assertEqual(opener.requests[1].full_url, prefix + '/' + opener.run_id)
        self.assertEqual(json.loads(opener.requests[0].data)['conf'], conf)

    def test_demo_sequences_runs_and_stops_on_failure(self):
        from contextlib import redirect_stdout
        spec = importlib.util.spec_from_file_location('run_pipeline', '/opt/world-bank/scripts/run-pipeline.py')
        client = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(client)

        for outcomes, expected_calls, result in (([1], 1, 1), ([0, 1], 2, 1), ([0, 0], 2, 0)):
            output = io.StringIO()
            with self.subTest(outcomes=outcomes), patch.object(client, 'trigger_and_wait',
                    side_effect=outcomes) as run, patch.dict(client.os.environ,
                    {'WORLD_BANK_EXPORTS_HOST_DIR': '/tmp/reports'}), redirect_stdout(output):
                self.assertEqual(client.demo('http://127.0.0.1:8080', 30), result)
            self.assertEqual(run.call_count, expected_calls)
            self.assertEqual(run.call_args_list[0].args, ('http://127.0.0.1:8080', {}))
            if expected_calls == 2:
                conf = run.call_args_list[1].args[1]
                self.assertEqual({key: value for key, value in conf.items() if key != 'ingestion_id'},
                                 {'mode': 'extract', 'countries': ['BRA'],
                                  'indicators': ['SP.POP.TOTL'], 'start_year': 2021, 'end_year': 2021})
                self.assertTrue(conf['ingestion_id'].startswith('demo_backfill_'))
                self.assertEqual(run.call_args_list[1].kwargs['dag'], 'world_bank_backfill')
            reports = [line for line in output.getvalue().splitlines() if line.startswith('/tmp/reports/')]
            self.assertEqual(len(reports), 4 if result == 0 else 0)



if __name__ == '__main__':
    unittest.main()
