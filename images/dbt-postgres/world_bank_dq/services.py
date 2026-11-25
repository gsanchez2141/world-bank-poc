"""Durable quality findings and source data in the existing warehouse."""
from pathlib import Path

from psycopg.types.json import Jsonb

OWNED_CHECKS = (
    "population_positive",
    "gdp_per_capita_consistency",
    "observation_disappeared",
    "observation_disappearance_jump",
)


def initialize(runs):
    with runs.db.transaction():
        runs.db.execute("SELECT pg_advisory_xact_lock(hashtext('world_bank_pipeline_run_ddl'))")
        runs.db.execute(Path(__file__).parent.parent.joinpath("world_bank_analytics", "meta.sql").read_text())
        runs.db.execute("""ALTER TABLE meta.pipeline_run ADD COLUMN IF NOT EXISTS dq_status text
            NOT NULL DEFAULT 'pending' CHECK (dq_status IN ('pending','failed','complete'))""")
        runs.db.execute("ALTER TABLE meta.pipeline_run ADD COLUMN IF NOT EXISTS dq_jump_message_id text")
    runs.db.commit()


def set_status(runs, status):
    runs.db.execute("UPDATE meta.pipeline_run SET dq_status=%s WHERE ingestion_id=%s AND dq_status != 'complete'",
                    (status, runs.id))
    runs.db.commit()


def facts(db):
    return db.execute("""
        SELECT c.country_iso3, i.indicator_code, y.year, f.value, f.ingestion_id, f.raw_ingested_at,
               source.raw_ingested_at AS source_raw_ingested_at
        FROM curated.fact_indicator_year f
        JOIN curated.dim_country c ON c.country_sk=f.country_sk
        JOIN curated.dim_indicator i ON i.indicator_sk=f.indicator_sk
        JOIN curated.dim_year y ON y.year_sk=f.year_sk
        LEFT JOIN meta.pipeline_run source ON source.ingestion_id=f.ingestion_id
    """).fetchall()


def prior_evidence(db):
    return db.execute("""
        SELECT s.country_iso3, s.indicator_code, s.year, s.value, s.ingestion_id, s.raw_ingested_at,
               source.raw_ingested_at AS source_raw_ingested_at
        FROM stg.observation s
        JOIN meta.pipeline_run load ON load.ingestion_id=s.consumer_run_id
        LEFT JOIN meta.pipeline_run source ON source.ingestion_id=s.ingestion_id
        WHERE load.consumer_status='complete' AND s.change_kind='append'
    """).fetchall()


def baselines(db):
    return db.execute("""
        SELECT r.ingestion_id, r.mode, r.raw_status, r.dq_status, r.raw_ingested_at, r.request_countries, r.request_indicators,
               r.start_year, r.end_year, count(d.ingestion_id)::integer AS warning_count
        FROM meta.pipeline_run r
        LEFT JOIN meta.dq_result d ON d.ingestion_id=r.ingestion_id
            AND d.check_name='observation_disappeared' AND d.severity='warning'
        WHERE r.mode='extract' AND r.raw_status='complete' AND r.dq_status='complete'
        GROUP BY r.ingestion_id
    """).fetchall()


def replace_findings(db, ingestion_id, findings):
    db.execute("DELETE FROM meta.dq_result WHERE ingestion_id=%s AND check_name=ANY(%s)",
               (ingestion_id, list(OWNED_CHECKS)))
    for finding in findings:
        db.execute("""INSERT INTO meta.dq_result
            (ingestion_id,check_name,severity,business_key,message,detected_at)
            VALUES (%s,%s,%s,%s,%s,clock_timestamp())""",
            (ingestion_id, finding["check_name"], finding["severity"],
             Jsonb(finding["business_key"]), finding["message"]))
