"""Independent PostgreSQL acceptance for the four reporting views."""
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

import psycopg
from psycopg import sql
from world_bank_analytics import views


class ReportingViews(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database = 'curated_test_' + uuid4().hex
        cls.admin = psycopg.connect(host='postgres', dbname='postgres', user='postgres',
                                    password='postgres', autocommit=True)
        cls.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.database)))
        try:
            cls.db = psycopg.connect(host='postgres', dbname=cls.database, user='postgres',
                                     password='postgres')
            cls.db.execute(Path('/usr/app/fixture/pipeline_run.sql').read_text())
            cls.db.execute("""
                CREATE SCHEMA curated;
                CREATE TABLE curated.dim_income_level(income_level_sk bigint, income_level_id text, income_level_name text);
                CREATE TABLE curated.dim_country(country_sk bigint, country_iso3 text, country_name text, income_level_sk bigint);
                CREATE TABLE curated.dim_indicator(indicator_sk bigint, indicator_code text, indicator_name text);
                CREATE TABLE curated.dim_year(year_sk bigint, year integer);
                CREATE TABLE curated.fact_indicator_year(country_sk bigint, indicator_sk bigint, year_sk bigint, value numeric(38,18));
                INSERT INTO curated.dim_income_level VALUES (1,'UMC','Upper middle income');
                INSERT INTO curated.dim_country VALUES (1,'BRA','Brazil',1),(2,'CAN','Canada',1),(3,'DEU','Germany',1);
                INSERT INTO curated.dim_indicator VALUES (1,'SP.POP.TOTL','Population'),(2,'NY.GDP.MKTP.CD','GDP'),
                    (3,'NY.GDP.PCAP.CD','GDP per capita'),(4,'SP.DYN.LE00.IN','Life expectancy');
                INSERT INTO curated.dim_year VALUES (2019,2019),(2020,2020),(2021,2021);
                INSERT INTO curated.fact_indicator_year VALUES
                    (1,1,2019,100),(1,1,2021,200),(1,2,2019,900),(1,2,2020,0),(1,2,2021,1000),
                    (1,4,2021,70),(2,1,2021,100),(2,2,2021,800),(2,4,2021,80),
                    (2,4,2020,NULL),(3,2,2021,600);
                INSERT INTO meta.pipeline_run(ingestion_id,mode,request_countries,request_indicators,
                    start_year,end_year,raw_status,raw_ingested_at)
                VALUES ('old','extract',ARRAY['BRA','CAN','DEU'],
                    ARRAY['SP.POP.TOTL','NY.GDP.MKTP.CD','NY.GDP.PCAP.CD','SP.DYN.LE00.IN'],
                    2019,2021,'complete','2025-01-01T00:00:00Z');
            """)
            cls.db.commit()
            cls.config = tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False)
            cls.config.write("countries: [BRA, CAN, DEU]\nindicators: [SP.POP.TOTL, NY.GDP.MKTP.CD, NY.GDP.PCAP.CD, SP.DYN.LE00.IN]\nstart_year: 2019\nend_year: 2021\n")
            cls.config.close()
            views.refresh(cls.db, cls.config.name)
            cls.db.commit()
        except BaseException:
            if hasattr(cls, 'db'):
                cls.db.close()
            cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.database)))
            cls.admin.close()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.db.close()
        cls.admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.database)))
        cls.admin.close()
        Path(cls.config.name).unlink(missing_ok=True)

    def rows(self, query, params=()):
        return self.db.execute(query, params).fetchall()

    def test_missingness_warning_scope_and_precedence(self):
        missing = {(c,i,y):reason for c,i,y,_,reason in self.rows('SELECT * FROM curated.vw_missing_or_incomplete')}
        self.assertEqual(missing[('BRA','SP.POP.TOTL',2020)], 'NEVER_RETURNED')
        self.assertEqual(missing[('CAN','SP.DYN.LE00.IN',2020)], 'SOURCE_NULL')
        self.db.execute("""
            INSERT INTO meta.dq_result VALUES
             ('old','observation_disappeared','warning','{"country_iso3":"BRA","indicator_code":"NY.GDP.MKTP.CD","year":2021}', 'missing',now()),
             ('old','observation_disappeared','warning','{"country_iso3":"CAN","indicator_code":"SP.DYN.LE00.IN","year":2020}', 'missing',now());
        """)
        def reason(country, indicator, year):
            return self.rows('SELECT reason FROM curated.vw_missing_or_incomplete WHERE country_iso3=%s AND indicator_code=%s AND year=%s', (country,indicator,year))
        self.assertEqual(reason('BRA','NY.GDP.MKTP.CD',2021), [('MISSING_FROM_LATEST_EXTRACT',)])
        self.assertEqual(reason('CAN','SP.DYN.LE00.IN',2020), [('MISSING_FROM_LATEST_EXTRACT',)])
        self.db.execute("""INSERT INTO meta.pipeline_run(ingestion_id,mode,request_countries,request_indicators,start_year,end_year,raw_status,raw_ingested_at)
             VALUES ('unrelated','extract',ARRAY['DEU'],ARRAY['NY.GDP.MKTP.CD'],2021,2021,'complete','2025-01-02')""")
        self.assertEqual(reason('BRA','NY.GDP.MKTP.CD',2021), [('MISSING_FROM_LATEST_EXTRACT',)])
        self.db.execute("""INSERT INTO meta.pipeline_run(ingestion_id,mode,request_countries,request_indicators,start_year,end_year,raw_status,raw_ingested_at)
             VALUES ('new','extract',ARRAY['BRA'],ARRAY['NY.GDP.MKTP.CD'],2021,2021,'complete','2025-01-03')""")
        self.assertEqual(reason('BRA','NY.GDP.MKTP.CD',2021), [])
        self.db.rollback()

    def test_year_and_latest_comparison(self):
        def yoy(country, indicator, year):
            return self.rows('SELECT yoy_change_pct,reason FROM curated.vw_yoy_change WHERE country_iso3=%s AND indicator_code=%s AND year=%s', (country,indicator,year))[0]
        self.assertEqual(yoy('BRA','SP.POP.TOTL',2021)[1], 'PRIOR_YEAR_MISSING')
        self.assertEqual(yoy('BRA','NY.GDP.MKTP.CD',2021)[1], 'PRIOR_YEAR_ZERO')
        self.assertEqual(yoy('CAN','SP.DYN.LE00.IN',2020)[1], 'CURRENT_NULL')
        self.assertEqual(yoy('BRA','NY.GDP.MKTP.CD',2020), (Decimal('-100'), None))
        rows = self.rows("SELECT country_iso3,value FROM curated.vw_latest_year_comparison WHERE indicator_code='SP.DYN.LE00.IN' ORDER BY country_iso3")
        self.assertEqual(rows, [('BRA',Decimal(70)),('CAN',Decimal(80)),('DEU',None)])

    def test_income_aggregations_and_coverage(self):
        rows = self.rows("""SELECT indicator_code,aggregation_method,value,expected_country_count,
                 available_country_count,coverage_pct FROM curated.vw_income_group_year
                 WHERE income_level_id='UMC' AND year=2021""")
        result = {row[0]:row[1:] for row in rows}
        coverage = Decimal('66.666666666667')
        self.assertEqual(result['SP.POP.TOTL'], ('SUM',Decimal(300),3,2,coverage))
        self.assertEqual(result['NY.GDP.MKTP.CD'], ('SUM',Decimal(2400),3,3,Decimal(100)))
        self.assertEqual(result['NY.GDP.PCAP.CD'], ('GDP_PER_CAPITA_RATIO',Decimal(6),3,2,coverage))
        self.assertEqual(result['SP.DYN.LE00.IN'], ('POPULATION_WEIGHTED_MEAN',Decimal('73.333333333333'),3,2,coverage))
        empty = self.rows("""SELECT value,available_country_count,coverage_pct FROM curated.vw_income_group_year
                WHERE income_level_id='UMC' AND year=2019 AND indicator_code='SP.DYN.LE00.IN'""")
        self.assertEqual(empty, [(None,0,Decimal(0))])


if __name__ == '__main__':
    unittest.main()
