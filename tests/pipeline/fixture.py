PROVISION = '''import boto3, configparser
from botocore.config import Config
p=configparser.RawConfigParser(); p.read('/opt/emulator/emulator_credentials')
s=boto3.Session(aws_access_key_id=p['default']['aws_access_key_id'],aws_secret_access_key=p['default']['aws_secret_access_key'],region_name='us-east-1')
def c(n): return s.client(n,endpoint_url='https://127.0.0.1:9297',verify='/opt/emulator/ca.pem',config=Config(proxies={},s3={'addressing_style':'path'}))
for b in ['lakehouse-raw','lakehouse-stage','lakehouse-curated']: c('s3').create_bucket(Bucket=b)
for g in ['/world-bank/airflow','/world-bank/lakehouse-ingest-world-bank','/world-bank/lakehouse-stage-world-bank','/world-bank/lakehouse-consumer-postgres','/world-bank/dbt-postgres']: c('logs').create_log_group(logGroupName=g)
c('sns').create_topic(Name='world-bank-alerts')'''


import base64
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]

class PrivatePipeline:
    def __enter__(self):
        self.project = 'wbbackfill' + uuid4().hex[:12]
        scratch = ROOT / '.handoff'
        scratch.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='wb-backfill-', dir=scratch)
        self.home = Path(self.temp.name)
        (self.home / '.aws/emulator').mkdir(parents=True)
        (self.home / 'control').write_text('good')
        self.env = dict(os.environ, PIPELINE_FIXTURE_HOME=str(self.home),
                        WORLD_BANK_EMULATOR_HOST_DIR=str(self.home / '.aws/emulator/9297'),
                        WORLD_BANK_EMULATOR_CONTAINER=f'{self.project}-aws-emulator-1',
                        WORLD_BANK_TEST_API_URL='http://127.0.0.1:18765/v2')
        self.compose = ['docker','compose','-p',self.project,'-f','compose.infra.yaml',
                        '-f','tests/pipeline/compose.fixture.yaml']
        try:
            self.call('up','-d','--no-build','--wait',timeout=360)
            self.call('exec','-T','airflow-scheduler','python','-c',PROVISION)
            port = self.call('port','airflow-webserver','8080').stdout.strip().rsplit(':',1)[-1]
            self.base = f'http://127.0.0.1:{port}/api/v1/dags/'
            self.auth = base64.b64encode(b'airflow:airflow').decode()
            deadline = time.monotonic()+120
            for dag in ('world_bank_pipeline','world_bank_backfill'):
                while True:
                    try:
                        self.request(dag)
                        break
                    except Exception:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(2)
            return self
        except BaseException:
            self.__exit__(None,None,None)
            raise

    def __exit__(self, *args):
        self.call('down','--volumes','--remove-orphans',check=False,timeout=120)
        self.temp.cleanup()

    def call(self, *args, check=True, timeout=240):
        return subprocess.run([*self.compose,*args],cwd=ROOT,env=self.env,text=True,
                              stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=check,timeout=timeout)

    def sql(self, query):
        return self.call('exec','-T','postgres','psql','-U','postgres','-d','warehouse',
                         '-At','-v','ON_ERROR_STOP=1','-c',query).stdout.strip()

    def request(self, path, payload=None):
        headers={'Authorization':'Basic '+self.auth}
        data=None
        if payload is not None:
            headers['Content-Type']='application/json'
            data=json.dumps(payload).encode()
        req=urllib.request.Request(self.base+path,data=data,headers=headers)
        with urllib.request.urlopen(req,timeout=10) as response:
            return json.load(response)

    def trigger(self, dag, run_id, conf):
        return self.request(dag+'/dagRuns', {'dag_run_id':run_id,'conf':conf})

    def state(self, dag, run_id):
        return self.request(dag+'/dagRuns/'+run_id)['state']

    def wait(self, dag, run_id):
        deadline=time.monotonic()+360
        while time.monotonic()<deadline:
            state=self.state(dag,run_id)
            if state in ('success','failed'):
                return state
            time.sleep(2)
        raise TimeoutError('Airflow run did not finish')
