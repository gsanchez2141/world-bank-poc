CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS curated;

CREATE TABLE IF NOT EXISTS meta.expected_key (
    country_iso3 text NOT NULL,
    indicator_code text NOT NULL,
    year integer NOT NULL,
    PRIMARY KEY (country_iso3, indicator_code, year)
);

CREATE TABLE IF NOT EXISTS meta.dq_result (
    ingestion_id text NOT NULL,
    check_name text NOT NULL,
    severity text NOT NULL,
    business_key jsonb NOT NULL,
    message text NOT NULL,
    detected_at timestamptz NOT NULL,
    PRIMARY KEY (ingestion_id, check_name, business_key)
);
