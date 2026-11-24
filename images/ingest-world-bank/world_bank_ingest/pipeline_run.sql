CREATE SCHEMA IF NOT EXISTS meta;
CREATE TABLE IF NOT EXISTS meta.pipeline_run (
  ingestion_id text PRIMARY KEY,
  mode text NOT NULL CHECK (mode IN ('extract', 'replay', 'reprocess_commit', 'rebuild')),
  source_ingestion_id text,
  request_countries text[],
  request_indicators text[],
  start_year integer,
  end_year integer,
  raw_ingested_at timestamptz,
  stage_ingested_at timestamptz,
  raw_status text NOT NULL DEFAULT 'pending' CHECK (raw_status IN ('pending','complete','failed')),
  stage_status text NOT NULL DEFAULT 'pending' CHECK (stage_status IN ('pending','complete','failed')),
  consumer_status text NOT NULL DEFAULT 'pending' CHECK (consumer_status IN ('pending','complete','failed')),
  curated_status text NOT NULL DEFAULT 'pending' CHECK (curated_status IN ('pending','complete','failed')),
  export_status text NOT NULL DEFAULT 'pending' CHECK (export_status IN ('pending','complete','failed')),
  stage_snapshot_ids jsonb,
  stage_changed_count bigint,
  successful_attempt integer,
  raw_date date,
  raw_attempt_count integer NOT NULL DEFAULT 0
);
