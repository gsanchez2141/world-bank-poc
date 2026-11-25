"""Exercise the real writer DAG in a disposable Compose project."""
import base64
import json
import os
import re
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
import urllib.request
from uuid import uuid4
from fixture import PROVISION

ROOT = Path(__file__).resolve().parents[2]

class PipelineIntegration(unittest.TestCase):
    def test_real_scheduler_recovery(self):
        project = 'wbpipe' + uuid4().hex[:12]
        scratch = ROOT / '.handoff'
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='wb-pipeline-', dir=scratch) as directory:
            home = Path(directory)
            (home / '.aws/emulator').mkdir(parents=True)
            (home / 'control').write_text('good')
            (home / 'exports').mkdir()
            env = dict(os.environ, PIPELINE_FIXTURE_HOME=directory,
                       WORLD_BANK_EMULATOR_HOST_DIR=str(home / '.aws/emulator/9297'),
                       WORLD_BANK_EMULATOR_CONTAINER=f'{project}-aws-emulator-1',
                       WORLD_BANK_TEST_API_URL='http://127.0.0.1:18765/v2',
                       WORLD_BANK_EXPORTS_HOST_DIR=str(home / 'exports'))
            compose = ['docker', 'compose', '-p', project, '-f', 'compose.infra.yaml',
                       '-f', 'tests/pipeline/compose.fixture.yaml']
            def call(*args, check=True, timeout=240):
                return subprocess.run([*compose, *args], cwd=ROOT, env=env, text=True,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                      timeout=timeout, check=check)
            def sql(query):
                return call('exec', '-T', 'postgres', 'psql', '-U', 'postgres', '-d',
                            'warehouse', '-At', '-v', 'ON_ERROR_STOP=1', '-c', query).stdout.strip()
            try:
                call('up', '-d', '--no-build', '--wait', timeout=360)
                # boto3 runs inside the private network and reads only its mounted emulator keys.
                call('exec', '-T', 'airflow-scheduler', 'python', '-c', PROVISION)
                port = call('port', 'airflow-webserver', '8080').stdout.strip().rsplit(':', 1)[-1]
                env['WORLD_BANK_AIRFLOW_URL'] = f'http://127.0.0.1:{port}'
                api = env['WORLD_BANK_AIRFLOW_URL'] + '/api/v1/dags/world_bank_pipeline'
                auth = base64.b64encode(b'airflow:airflow').decode()
                def request(path):
                    req = urllib.request.Request(api + path, headers={'Authorization': 'Basic ' + auth})
                    with urllib.request.urlopen(req, timeout=10) as response:
                        return json.load(response)
                deadline = time.monotonic() + 120
                while True:
                    try:
                        dag = request('')
                        break
                    except Exception:
                        if time.monotonic() > deadline:
                            raise
                        time.sleep(2)
                self.assertEqual(dag['max_active_runs'], 1)
                base = {'countries':['BRA'], 'indicators':['SP.POP.TOTL'], 'start_year':2020, 'end_year':2021}
                def run(identifier, **extra):
                    conf = dict(base, ingestion_id=identifier, **extra)
                    current = dict(env, WORLD_BANK_RUN_CONF=json.dumps(conf))
                    return subprocess.run(['make', 'run'], cwd=ROOT, env=current, text=True,
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=360)
                first = run('fixture_good')
                self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
                self.assertEqual(sql("select raw_status||','||stage_status||','||consumer_status||','||curated_status from meta.pipeline_run where ingestion_id='fixture_good'"), 'complete,complete,complete,complete')
                self.assertEqual(sql("select export_status from meta.pipeline_run where ingestion_id='fixture_good'"), 'complete')
                basenames = ('vw_missing_or_incomplete','vw_yoy_change','vw_latest_year_comparison','vw_income_group_year')
                self.assertEqual({p.name for p in (home / 'exports').glob('*.csv')},
                                 {name + '.csv' for name in basenames})
                check_exports = '''import boto3,configparser,json
from botocore.config import Config
p=configparser.RawConfigParser();p.read('/opt/emulator/emulator_credentials')
s=boto3.Session(aws_access_key_id=p['default']['aws_access_key_id'],aws_secret_access_key=p['default']['aws_secret_access_key'],region_name='us-east-1')
c=s.client('s3',endpoint_url='https://127.0.0.1:9297',verify='/opt/emulator/ca.pem',config=Config(proxies={},s3={'addressing_style':'path'}))
import sys
identifier=sys.argv[1]
keys=[x['Key'] for x in c.list_objects_v2(Bucket='lakehouse-curated',Prefix='world-bank/reporting/').get('Contents',[]) if '/ingestion_id='+identifier+'/' in x['Key']]
print(json.dumps(keys))'''
                keys=json.loads(call('exec','-T','airflow-scheduler','python','-c',check_exports,'fixture_good').stdout)
                self.assertEqual({Path(key).name for key in keys},
                                 {name + ext for name in basenames for ext in ('.csv','.parquet')})
                # An imported pre-export ledger row does not block a new writer.
                sql("UPDATE meta.pipeline_run SET export_required=false, export_status='pending' WHERE ingestion_id='fixture_good'")
                target = home / 'exports' / 'vw_income_group_year.csv'
                target.unlink()
                target.mkdir()
                (home / 'control').write_text('good')
                export_failed=run('fixture_export_failure')
                self.assertNotEqual(export_failed.returncode,0,export_failed.stdout+export_failed.stderr)
                self.assertEqual(sql("select raw_status||','||stage_status||','||consumer_status||','||curated_status||','||export_status from meta.pipeline_run where ingestion_id='fixture_export_failure'"), 'complete,complete,complete,complete,failed')
                before_export=sql("select raw_attempt_count::text || ':' || stage_snapshot_ids::text from meta.pipeline_run where ingestion_id='fixture_export_failure'")
                blocked_export=run('fixture_export_blocked')
                self.assertNotEqual(blocked_export.returncode,0)
                self.assertEqual(sql("select count(*) from meta.pipeline_run where ingestion_id='fixture_export_blocked'"), '0')
                target.rmdir()
                current=dict(env,WORLD_BANK_RUN_CONF=json.dumps({'ingestion_id':'fixture_export_failure'}))
                export_resumed=subprocess.run(['make','run'],cwd=ROOT,env=current,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=360)
                self.assertEqual(export_resumed.returncode,0,export_resumed.stdout+export_resumed.stderr)
                self.assertEqual(sql("select export_status from meta.pipeline_run where ingestion_id='fixture_export_failure'"),'complete')
                self.assertEqual(sql("select raw_attempt_count::text || ':' || stage_snapshot_ids::text from meta.pipeline_run where ingestion_id='fixture_export_failure'"),before_export)
                export_logs = '''import boto3,configparser,json
from botocore.config import Config
p=configparser.RawConfigParser();p.read('/opt/emulator/emulator_credentials')
s=boto3.Session(aws_access_key_id=p['default']['aws_access_key_id'],aws_secret_access_key=p['default']['aws_secret_access_key'],region_name='us-east-1')
c=s.client('logs',endpoint_url='https://127.0.0.1:9297',verify='/opt/emulator/ca.pem',config=Config(proxies={}))
events=c.filter_log_events(logGroupName='/world-bank/airflow',logStreamNames=['fixture_export_failure'])['events']
print(json.dumps([json.loads(e['message']) for e in events]))'''
                events=json.loads(call('exec','-T','airflow-scheduler','python','-c',export_logs).stdout)
                self.assertTrue(any(e.get('event')=='pipeline_task_failed' and e.get('task_name')=='export' for e in events))
                self.assertTrue(any(e.get('event')=='alert_published' and e.get('message_id') for e in events))

                keys=json.loads(call('exec','-T','airflow-scheduler','python','-c',check_exports,'fixture_export_failure').stdout)
                self.assertEqual({Path(key).name for key in keys},{name+ext for name in basenames for ext in ('.csv','.parquet')})
                self.assertEqual({p.name for p in (home / 'exports').glob('*.csv')},{name+'.csv' for name in basenames})
                (home / 'control').write_text('good')
                match = re.search(r'airflow_run_id=(\S+)', first.stdout)
                self.assertIsNotNone(match, first.stdout)
                good_run = match.group(1)
                xcom = request('/dagRuns/' + good_run + '/taskInstances/gate/xcomEntries')
                self.assertEqual([entry['key'] for entry in xcom['xcom_entries']], ['return_value'])
                for task in ('ingest', 'stage', 'consumer', 'curated', 'dq', 'export'):
                    values = request('/dagRuns/' + good_run + '/taskInstances/' + task + '/xcomEntries')
                    self.assertEqual(values['xcom_entries'], [])

                (home / 'control').write_text('hold')
                def post(identifier):
                    payload = json.dumps({'dag_run_id': identifier, 'conf':dict(base, ingestion_id=identifier)}).encode()
                    req = urllib.request.Request(api + '/dagRuns', data=payload, method='POST',
                                                 headers={'Authorization':'Basic '+auth,'Content-Type':'application/json'})
                    with urllib.request.urlopen(req, timeout=10) as response:
                        return json.load(response)
                post('fixture_queue_one')
                deadline = time.monotonic() + 90
                while request('/dagRuns/fixture_queue_one')['state'] != 'running':
                    if time.monotonic() > deadline:
                        self.fail('first queued run never became active')
                    time.sleep(1)
                post('fixture_queue_two')
                time.sleep(5)
                self.assertEqual(request('/dagRuns/fixture_queue_two')['state'], 'queued')
                (home / 'control').write_text('good')
                for identifier in ('fixture_queue_one', 'fixture_queue_two'):
                    deadline = time.monotonic() + 300
                    while True:
                        state = request('/dagRuns/' + identifier)['state']
                        if state in ('success', 'failed'):
                            break
                        if time.monotonic() > deadline:
                            self.fail('queued DAG run timed out')
                        time.sleep(2)
                    self.assertEqual(state, 'success')
                (home / 'control').write_text('bad')
                bad = run('fixture_bad')
                self.assertNotEqual(bad.returncode, 0)
                match = re.search(r'airflow_run_id=(\S+)', bad.stdout)
                self.assertIsNotNone(match, bad.stdout)
                bad_run = match.group(1)
                tasks = request('/dagRuns/' + bad_run + '/taskInstances')['task_instances']
                states = {task['task_id']:task['state'] for task in tasks}
                self.assertEqual(states['ingest'], 'failed')
                for task in ('stage','consumer','curated','dq','export'):
                    self.assertIn(states[task], ('upstream_failed','skipped'))

                self.assertEqual(sql("select raw_status from meta.pipeline_run where ingestion_id='fixture_bad'"), 'failed')
                inspect_logs = '''import boto3, configparser, json
from botocore.config import Config
p=configparser.RawConfigParser();p.read('/opt/emulator/emulator_credentials')
s=boto3.Session(aws_access_key_id=p['default']['aws_access_key_id'],aws_secret_access_key=p['default']['aws_secret_access_key'],region_name='us-east-1')
c=s.client('logs',endpoint_url='https://127.0.0.1:9297',verify='/opt/emulator/ca.pem',config=Config(proxies={}))
for group in ['/world-bank/airflow','/world-bank/lakehouse-ingest-world-bank']:
 e=c.filter_log_events(logGroupName=group,logStreamNames=['fixture_bad'])['events']
 print(group, json.dumps([json.loads(x['message']) for x in e]))'''
                logs = call('exec','-T','airflow-scheduler','python','-c',inspect_logs).stdout.splitlines()
                airflow_events = json.loads(logs[0].split(' ', 1)[1])
                ingest_events = json.loads(logs[1].split(' ', 1)[1])
                self.assertEqual(sum(e.get('event') == 'alert_published' and bool(e.get('message_id')) for e in airflow_events), 1)
                self.assertTrue(any(e.get('event') == 'ingest_failed' for e in ingest_events))
                self.assertFalse(any(e.get('event') == 'alert_published' for e in ingest_events))
                blocked = run('fixture_blocked')
                self.assertNotEqual(blocked.returncode, 0)
                self.assertEqual(sql("select count(*) from meta.pipeline_run where ingestion_id='fixture_blocked'"), '0')
                (home / 'control').write_text('correction')
                sql("CREATE FUNCTION curated.fixture_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.ingestion_id = 'fixture_recovery' THEN RAISE EXCEPTION 'fixture curated failure'; END IF; RETURN NEW; END $$")
                sql("CREATE TRIGGER fixture_reject BEFORE UPDATE ON curated.fact_indicator_year FOR EACH ROW EXECUTE FUNCTION curated.fixture_reject()")
                recovered = run('fixture_recovery', abandon_previous=True)
                self.assertNotEqual(recovered.returncode, 0, recovered.stdout + recovered.stderr)
                state = sql("select raw_status||','||stage_status||','||consumer_status||','||curated_status from meta.pipeline_run where ingestion_id='fixture_recovery'")
                self.assertEqual(state, 'complete,complete,complete,failed')
                before = sql("select raw_attempt_count::text || ':' || stage_snapshot_ids::text from meta.pipeline_run where ingestion_id='fixture_recovery'")
                sql('DROP TRIGGER fixture_reject ON curated.fact_indicator_year')
                sql('DROP FUNCTION curated.fixture_reject()')
                current = dict(env, WORLD_BANK_RUN_CONF=json.dumps({'ingestion_id':'fixture_recovery'}))
                resumed = subprocess.run(['make','run'], cwd=ROOT, env=current, text=True,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=360)
                self.assertEqual(resumed.returncode, 0, resumed.stdout + resumed.stderr)
                self.assertEqual(before, sql("select raw_attempt_count::text || ':' || stage_snapshot_ids::text from meta.pipeline_run where ingestion_id='fixture_recovery'"))
                self.assertEqual(sql("select abandoned_by_ingestion_id from meta.pipeline_run where ingestion_id='fixture_bad'"), 'fixture_recovery')

                # A rebuild reads the current Iceberg set, including the 2020 row
                # absent from the correction, and cannot overwrite populated data.
                def rebuild():
                    current = dict(env, WORLD_BANK_RUN_CONF=json.dumps(
                        {'ingestion_id':'fixture_restore','mode':'rebuild'}))
                    return subprocess.run(['make','run'], cwd=ROOT, env=current, text=True,
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=360)
                head_script = '''import json, urllib.request
for table in ('observation','country','indicator','income_level'):
 with urllib.request.urlopen('http://127.0.0.1:8181/v1/namespaces/world_bank/tables/'+table) as r:
  meta=json.load(r)['metadata']
  print(table, meta.get('current-snapshot-id'))'''
                heads_before = call('exec','-T','airflow-scheduler','python','-c',head_script).stdout
                expected_source = sql("select year::text || ':' || coalesce(value::text,'NULL') || ':' || raw_response_key || ':' || ingestion_id || ':' || stage_run_id from stg.observation where consumer_run_id in ('fixture_good','fixture_recovery') and change_kind='append' and ((consumer_run_id='fixture_good' and year=2020) or (consumer_run_id='fixture_recovery' and year=2021)) order by year")
                existing_fact = sql("select count(*) from curated.fact_indicator_year")
                rejected = rebuild()
                self.assertNotEqual(rejected.returncode, 0)
                self.assertEqual(sql("select count(*) from curated.fact_indicator_year"), existing_fact)
                self.assertEqual(call('exec','-T','airflow-scheduler','python','-c',head_script).stdout, heads_before)

                sql('TRUNCATE stg.observation, stg.country, stg.indicator, stg.income_level, curated.fact_indicator_year, curated.dim_country, curated.dim_indicator, curated.dim_income_level, curated.dim_year')
                sql("CREATE FUNCTION curated.fixture_rebuild_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rebuild dbt failure'; END $$")
                sql("CREATE TRIGGER fixture_rebuild_reject BEFORE INSERT ON curated.fact_indicator_year FOR EACH ROW EXECUTE FUNCTION curated.fixture_rebuild_reject()")
                failed = rebuild()
                self.assertNotEqual(failed.returncode, 0, failed.stdout + failed.stderr)
                self.assertEqual(sql("select consumer_status||','||curated_status from meta.pipeline_run where ingestion_id='fixture_restore'"), 'complete,failed')
                pinned = sql("select rebuild_snapshot_ids::text from meta.pipeline_run where ingestion_id='fixture_restore'")
                pins = json.loads(pinned)
                expected_heads = {line.split()[0]:line.split()[1] for line in heads_before.splitlines()}
                self.assertEqual(pins, expected_heads)
                restored_source = sql("select year::text || ':' || coalesce(value::text,'NULL') || ':' || raw_response_key || ':' || ingestion_id || ':' || stage_run_id from stg.observation where consumer_run_id='fixture_restore' order by year")
                self.assertEqual(restored_source, expected_source)
                self.assertEqual(sql("select count(*) from stg.observation where consumer_run_id='fixture_restore'"), '2')
                self.assertEqual(sql("select count(*) from stg.observation where consumer_run_id='fixture_restore' and year=2020 and value is null"), '1')
                self.assertEqual(sql("select value::text from stg.observation where consumer_run_id='fixture_restore' and year=2021"), '43.000000000000000000')
                for table in ('country','indicator','income_level'):
                    self.assertGreater(int(sql("select count(*) from stg." + table + " where consumer_run_id='fixture_restore' and raw_response_key is not null and stage_run_id is not null")), 0)
                stage_before = sql("select json_agg(row_to_json(t) order by year)::text from (select * from stg.observation where consumer_run_id='fixture_restore') t")
                sql('DROP TRIGGER fixture_rebuild_reject ON curated.fact_indicator_year')
                sql('DROP FUNCTION curated.fixture_rebuild_reject()')
                restored = rebuild()
                self.assertEqual(restored.returncode, 0, restored.stdout + restored.stderr)
                self.assertEqual(sql("select consumer_status||','||curated_status from meta.pipeline_run where ingestion_id='fixture_restore'"), 'complete,complete')
                self.assertEqual(sql("select rebuild_snapshot_ids::text from meta.pipeline_run where ingestion_id='fixture_restore'"), pinned)
                self.assertEqual(sql("select json_agg(row_to_json(t) order by year)::text from (select * from stg.observation where consumer_run_id='fixture_restore') t"), stage_before)
                self.assertEqual(sql("select count(*) from curated.fact_indicator_year"), '2')
                self.assertEqual(call('exec','-T','airflow-scheduler','python','-c',head_script).stdout, heads_before)
                check_artifacts = '''import boto3,configparser
from botocore.config import Config
p=configparser.RawConfigParser();p.read('/opt/emulator/emulator_credentials')
s=boto3.Session(aws_access_key_id=p['default']['aws_access_key_id'],aws_secret_access_key=p['default']['aws_secret_access_key'],region_name='us-east-1')
c=s.client('s3',endpoint_url='https://127.0.0.1:9297',verify='/opt/emulator/ca.pem',config=Config(proxies={},s3={'addressing_style':'path'}))
print('\\n'.join(x['Key'] for x in c.list_objects_v2(Bucket='lakehouse-curated',Prefix='world-bank/dbt_catalog/').get('Contents',[]) if '/ingestion_id=fixture_restore/' in x['Key']))'''
                artifact_result = call('exec','-T','airflow-scheduler','python','-c',check_artifacts, check=False)
                self.assertEqual(artifact_result.returncode, 0, artifact_result.stderr)
                artifacts = artifact_result.stdout.splitlines()
                self.assertEqual({Path(key).name for key in artifacts}, {'manifest.json','catalog.json'})
                pinned_day = sql("select (rebuild_pinned_at at time zone 'UTC')::date::text from meta.pipeline_run where ingestion_id='fixture_restore'")
                self.assertTrue(all('/date=' + pinned_day + '/ingestion_id=fixture_restore/' in key for key in artifacts))

            finally:
                call('down', '--volumes', '--remove-orphans', check=False, timeout=120)

if __name__ == '__main__':
    unittest.main()
