"""Real writer quality checks with scoped saved responses."""
import csv
import json
from pathlib import Path
import unittest

from fixture import PrivatePipeline

INDICATORS = ['SP.POP.TOTL','NY.GDP.MKTP.CD','NY.GDP.PCAP.CD']


class QualityIntegration(unittest.TestCase):
    def test_scope_warning_and_numeric_resume(self):
        with PrivatePipeline() as f:
            def run(identifier, countries, mode, expected="success"):
                (f.home / 'control').write_text(mode)
                conf = {'ingestion_id':identifier,'countries':countries,'indicators':INDICATORS,
                        'start_year':2020,'end_year':2021}
                run_id = 'manual_' + identifier
                f.trigger('world_bank_pipeline',run_id,conf)
                result = f.wait('world_bank_pipeline',run_id)
                if result != expected:
                    tasks=f.request('world_bank_pipeline/dagRuns/'+run_id+'/taskInstances')['task_instances']
                    print('fixture DAG failure',run_id,[(t['task_id'],t['state']) for t in tasks],flush=True)
                    for task in tasks:
                        if task['state'] != 'failed':
                            continue
                        cloud_logs = '''import boto3,configparser,json,sys
from botocore.config import Config
p=configparser.RawConfigParser();p.read('/opt/emulator/emulator_credentials')
s=boto3.Session(aws_access_key_id=p['default']['aws_access_key_id'],aws_secret_access_key=p['default']['aws_secret_access_key'],region_name='us-east-1')
c=s.client('logs',endpoint_url='https://127.0.0.1:9297',verify='/opt/emulator/ca.pem',config=Config(proxies={}))
for group in ['/world-bank/airflow','/world-bank/dbt-postgres','/world-bank/lakehouse-stage-world-bank']:
 try:
  events=c.filter_log_events(logGroupName=group,logStreamNames=[sys.argv[1]])['events']
  print(group,json.dumps([e['message'] for e in events]))
 except Exception as e: print(group,type(e).__name__)
'''
                        evidence=f.call('exec','-T','airflow-scheduler','python','-c',cloud_logs,identifier,check=False)
                        print('fixture cloud log context',evidence.stdout[-15000:],evidence.stderr[-2000:],flush=True)
                return result
            self.assertEqual(run('dq_seed',['BRA','CAN'],'dq_seed'),'success')
            self.assertEqual(run('dq_baseline',['BRA'],'dq_seed'),'success')
            self.assertEqual(f.sql("select count(*) from meta.dq_result where ingestion_id='dq_baseline' and check_name='observation_disappeared'"),'0')
            before = f.sql("""select i.indicator_code || ':' || fa.value::text || ':' || fa.ingestion_id
                from curated.fact_indicator_year fa join curated.dim_country c using(country_sk)
                join curated.dim_indicator i using(indicator_sk) join curated.dim_year y using(year_sk)
                where c.country_iso3='BRA' and y.year=2021 and i.indicator_code in ('NY.GDP.MKTP.CD','NY.GDP.PCAP.CD')
                order by i.indicator_code""")
            self.assertEqual(run('dq_omit',['BRA'],'dq_omit'),'success')
            rows = json.loads(f.sql("""select coalesce(json_agg(business_key order by business_key::text),'[]'::json)::text
                from meta.dq_result where ingestion_id='dq_omit' and check_name='observation_disappeared'"""))
            self.assertEqual({(r['country_iso3'],r['indicator_code'],r['year']) for r in rows},
                             {('BRA','NY.GDP.MKTP.CD',2021),('BRA','NY.GDP.PCAP.CD',2021)})
            after = f.sql("""select i.indicator_code || ':' || fa.value::text || ':' || fa.ingestion_id
                from curated.fact_indicator_year fa join curated.dim_country c using(country_sk)
                join curated.dim_indicator i using(indicator_sk) join curated.dim_year y using(year_sk)
                where c.country_iso3='BRA' and y.year=2021 and i.indicator_code in ('NY.GDP.MKTP.CD','NY.GDP.PCAP.CD')
                order by i.indicator_code""")
            self.assertEqual(after,before)
            ack = f.sql("select coalesce(dq_jump_message_id,'') from meta.pipeline_run where ingestion_id='dq_omit'")
            self.assertTrue(ack)
            self.assertEqual(f.sql("select count(*) from meta.dq_result where ingestion_id='dq_omit' and check_name='observation_disappearance_jump'"),'1')
            with (f.home / 'exports' / 'vw_missing_or_incomplete.csv').open(newline='') as handle:
                exported=list(csv.DictReader(handle))
            self.assertEqual(sum(row['country_iso3']=='BRA' and row['year']=='2021' and
                                 row['reason']=='MISSING_FROM_LATEST_EXTRACT' for row in exported),2)
            reasons=f.sql("""select indicator_code||':'||reason from curated.vw_missing_or_incomplete
                where country_iso3='BRA' and year=2021 and indicator_code in ('NY.GDP.MKTP.CD','NY.GDP.PCAP.CD') order by indicator_code""")
            self.assertEqual(reasons.count('MISSING_FROM_LATEST_EXTRACT'),2)
            self.assertEqual(f.sql("select reason from curated.vw_missing_or_incomplete where country_iso3='BRA' and indicator_code='SP.POP.TOTL' and year=2020"),'SOURCE_NULL')
            self.assertEqual(f.sql("select reason from curated.vw_missing_or_incomplete where country_iso3='DEU' and indicator_code='SP.POP.TOTL' and year=2020"),'NEVER_RETURNED')
            f.trigger('world_bank_pipeline','retry_dq_omit',{'ingestion_id':'dq_omit'})
            self.assertEqual(f.wait('world_bank_pipeline','retry_dq_omit'),'success')
            self.assertEqual(f.sql("select dq_jump_message_id from meta.pipeline_run where ingestion_id='dq_omit'"),ack)
            self.assertEqual(f.sql("select count(*) from meta.dq_result where ingestion_id='dq_omit' and check_name='observation_disappeared'"),'2')
            self.assertEqual(run('dq_recover',['BRA'],'dq_seed'),'success')
            self.assertEqual(f.sql("select count(*) from curated.vw_missing_or_incomplete where country_iso3='BRA' and year=2021 and indicator_code in ('NY.GDP.MKTP.CD','NY.GDP.PCAP.CD')"),'0')
            self.assertEqual(run('dq_unrelated',['CAN'],'dq_seed'),'success')
            self.assertEqual(f.sql("select count(*) from meta.dq_result where ingestion_id='dq_unrelated' and check_name='observation_disappearance_jump'"),'0')

            f.sql("""CREATE FUNCTION curated.fixture_quality_corrupt() RETURNS trigger LANGUAGE plpgsql AS $$
                DECLARE indicator text; yr integer;
                BEGIN
                  SELECT indicator_code INTO indicator FROM curated.dim_indicator WHERE indicator_sk=NEW.indicator_sk;
                  SELECT year INTO yr FROM curated.dim_year WHERE year_sk=NEW.year_sk;
                  IF NEW.ingestion_id='dq_numeric' AND indicator='SP.POP.TOTL' AND yr=2020 THEN NEW.value=0; END IF;
                  IF NEW.ingestion_id='dq_numeric' AND indicator='NY.GDP.PCAP.CD' AND yr=2021 THEN NEW.value=99; END IF;
                  RETURN NEW;
                END $$""")
            f.sql("CREATE TRIGGER fixture_quality_corrupt BEFORE INSERT OR UPDATE ON curated.fact_indicator_year FOR EACH ROW EXECUTE FUNCTION curated.fixture_quality_corrupt()")
            self.assertEqual(run('dq_numeric',['BRA'],'dq_numeric',expected='failed'),'failed')
            statuses=f.sql("select raw_status||','||stage_status||','||consumer_status||','||curated_status||','||dq_status||','||export_status from meta.pipeline_run where ingestion_id='dq_numeric'")
            self.assertEqual(statuses,'complete,complete,complete,complete,failed,pending')
            tasks=f.request('world_bank_pipeline/dagRuns/manual_dq_numeric/taskInstances')['task_instances']
            states={task['task_id']:task['state'] for task in tasks}
            self.assertEqual(states['dq'],'failed')
            self.assertIn(states['export'],('upstream_failed','skipped'))
            self.assertEqual(f.request('world_bank_pipeline/dagRuns/manual_dq_numeric/taskInstances/dq/xcomEntries')['xcom_entries'],[])
            get_logs = '''import boto3,configparser,json
from botocore.config import Config
p=configparser.RawConfigParser();p.read('/opt/emulator/emulator_credentials')
s=boto3.Session(aws_access_key_id=p['default']['aws_access_key_id'],aws_secret_access_key=p['default']['aws_secret_access_key'],region_name='us-east-1')
c=s.client('logs',endpoint_url='https://127.0.0.1:9297',verify='/opt/emulator/ca.pem',config=Config(proxies={}))
print(json.dumps([json.loads(x['message']) for x in c.filter_log_events(logGroupName='/world-bank/airflow',logStreamNames=['dq_numeric'])['events']]))'''
            events=json.loads(f.call('exec','-T','airflow-scheduler','python','-c',get_logs).stdout)
            self.assertTrue(any(e.get('event')=='alert_published' and e.get('message_id') for e in events))
            checks=f.sql("select string_agg(check_name,',' order by check_name) from meta.dq_result where ingestion_id='dq_numeric' and severity='error'")
            self.assertIn('population_positive',checks)
            self.assertIn('gdp_per_capita_consistency',checks)
            before_retry=f.sql("select raw_attempt_count::text||':'||stage_snapshot_ids::text from meta.pipeline_run where ingestion_id='dq_numeric'")
            f.sql('DROP TRIGGER fixture_quality_corrupt ON curated.fact_indicator_year')
            f.sql('DROP FUNCTION curated.fixture_quality_corrupt()')
            f.sql("""update curated.fact_indicator_year f set value=case
                when i.indicator_code='SP.POP.TOTL' then 100 else 11 end
                from curated.dim_indicator i, curated.dim_year y, curated.dim_country c
                where f.indicator_sk=i.indicator_sk and f.year_sk=y.year_sk and f.country_sk=c.country_sk
                and c.country_iso3='BRA' and ((i.indicator_code='SP.POP.TOTL' and y.year=2020)
                  or (i.indicator_code='NY.GDP.PCAP.CD' and y.year=2021))""")
            f.trigger('world_bank_pipeline','retry_dq_numeric',{'ingestion_id':'dq_numeric'})
            self.assertEqual(f.wait('world_bank_pipeline','retry_dq_numeric'),'success')
            self.assertEqual(f.sql("select raw_attempt_count::text||':'||stage_snapshot_ids::text from meta.pipeline_run where ingestion_id='dq_numeric'"),before_retry)
            self.assertEqual(f.sql("select dq_status||','||export_status from meta.pipeline_run where ingestion_id='dq_numeric'"),'complete,complete')
            self.assertEqual(f.sql("select count(*) from meta.dq_result where ingestion_id='dq_numeric' and severity='error'"),'0')


if __name__ == '__main__':
    unittest.main()
