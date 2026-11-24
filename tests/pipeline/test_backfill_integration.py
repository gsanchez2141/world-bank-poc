"""Run the real backfill parent and writer in a private local stack."""
import hashlib
import time
import unittest

from fixture import PrivatePipeline


class BackfillIntegration(unittest.TestCase):
    def test_parent_dispatch_and_writer_modes(self):
        with PrivatePipeline() as f:
            common = {'countries':['BRA'], 'indicators':['SP.POP.TOTL'],
                      'start_year':2020, 'end_year':2021}
            f.trigger('world_bank_pipeline','seed',dict(common, ingestion_id='seed'))
            self.assertEqual(f.wait('world_bank_pipeline','seed'), 'success')
            baseline = f.sql("select coalesce(value::text, 'NULL') || ':' || version::text || ':' || raw_response_key from stg.observation where consumer_run_id='seed' and year=2020")

            (f.home / 'control').write_text('hold')
            narrow_2020 = dict(common, start_year=2020, end_year=2020, ingestion_id='held')
            f.trigger('world_bank_pipeline','held',narrow_2020)
            deadline=time.monotonic()+90
            while f.state('world_bank_pipeline','held') != 'running':
                if time.monotonic()>deadline:
                    self.fail('writer never became active')
                time.sleep(1)
            narrow_2021 = dict(common, start_year=2021, end_year=2021, ingestion_id='correction')
            parent='parent_extract'
            child='manual__wbbackfill_'+hashlib.sha256(parent.encode()).hexdigest()
            f.trigger('world_bank_backfill',parent,narrow_2021)
            deadline=time.monotonic()+90
            while True:
                try:
                    child_state=f.state('world_bank_pipeline',child)
                    break
                except Exception:
                    if time.monotonic()>deadline:
                        raise
                    time.sleep(1)
            self.assertEqual(child_state,'queued')
            self.assertEqual(f.request('world_bank_pipeline/dagRuns/'+child)['run_type'], 'manual')
            (f.home / 'control').write_text('correction')
            self.assertEqual(f.wait('world_bank_pipeline','held'),'success')
            self.assertEqual(f.wait('world_bank_backfill',parent),'success')
            self.assertEqual(f.wait('world_bank_pipeline',child),'success')
            children=f.request('world_bank_pipeline/dagRuns?limit=100')['dag_runs']
            self.assertEqual(sum(run['dag_run_id']==child for run in children),1)
            self.assertEqual(f.sql("select coalesce(value::text, 'NULL') || ':' || version::text || ':' || raw_response_key from stg.observation where consumer_run_id='seed' and year=2020"),baseline)
            self.assertEqual(f.sql("select value::text from stg.observation where consumer_run_id='correction' and year=2021 and change_kind='append'"),'43.000000000000000000')

            for mode, source, identifier in (('replay','seed','replay'),('reprocess_commit','correction','reprocess')):
                parent_id='parent_'+identifier
                expected='manual__wbbackfill_'+hashlib.sha256(parent_id.encode()).hexdigest()
                f.trigger('world_bank_backfill',parent_id,{'mode':mode,'source_ingestion_id':source,'ingestion_id':identifier})
                self.assertEqual(f.wait('world_bank_backfill',parent_id),'success')
                self.assertEqual(f.wait('world_bank_pipeline',expected),'success')
                self.assertEqual(f.sql("select mode||':'||source_ingestion_id from meta.pipeline_run where ingestion_id='"+identifier+"'"), mode+':'+source)
            self.assertEqual(f.sql("select raw_attempt_count from meta.pipeline_run where ingestion_id='replay'"),'0')
            self.assertEqual(f.sql("select raw_status||':'||stage_status from meta.pipeline_run where ingestion_id='reprocess'"),'pending:pending')

            f.trigger('world_bank_backfill','parent_restore_bad',{'mode':'rebuild','ingestion_id':'restore'})
            bad='manual__wbbackfill_'+hashlib.sha256(b'parent_restore_bad').hexdigest()
            self.assertEqual(f.wait('world_bank_backfill','parent_restore_bad'),'failed')
            self.assertEqual(f.wait('world_bank_pipeline',bad),'failed')
            self.assertEqual(f.sql('select count(*) from curated.fact_indicator_year'),'2')
            f.sql('TRUNCATE stg.observation, stg.country, stg.indicator, stg.income_level, curated.fact_indicator_year, curated.dim_country, curated.dim_indicator, curated.dim_income_level, curated.dim_year')
            f.trigger('world_bank_backfill','parent_restore_good',{'mode':'rebuild','ingestion_id':'restore'})
            good='manual__wbbackfill_'+hashlib.sha256(b'parent_restore_good').hexdigest()
            self.assertEqual(f.wait('world_bank_backfill','parent_restore_good'),'success')
            self.assertEqual(f.wait('world_bank_pipeline',good),'success')
            self.assertEqual(f.sql('select count(*) from curated.fact_indicator_year'),'2')
            self.assertEqual(f.sql("select count(*) from stg.observation where consumer_run_id='restore' and year=2020 and value is null"),'1')
            self.assertEqual(f.sql("select value::text from stg.observation where consumer_run_id='restore' and year=2021"),'43.000000000000000000')


if __name__ == '__main__':
    unittest.main()
