-- PHASE attach
LOAD httpfs;
LOAD iceberg;
SET ca_cert_file = '/opt/emulator/ca.pem';
SET enable_server_cert_verification = true;
SET enable_progress_bar = false;
CREATE OR REPLACE SECRET s3_emulator (
    TYPE s3,
    PROVIDER config,
    KEY_ID getenv('EMULATOR_ACCESS_KEY_ID'),
    SECRET getenv('EMULATOR_SECRET_ACCESS_KEY'),
    REGION 'us-east-1',
    ENDPOINT '127.0.0.1:9297',
    URL_STYLE 'path',
    USE_SSL true
);
ATTACH 'lakehouse-stage' AS ice (
    TYPE iceberg,
    URI 'http://127.0.0.1:8181',
    AUTHORIZATION_TYPE 'none'
);

-- PHASE setup
CREATE SCHEMA IF NOT EXISTS ice.spike;
DROP TABLE IF EXISTS ice.spike.scd2_probe;
CREATE TABLE ice.spike.scd2_probe (
    business_key VARCHAR,
    payload VARCHAR,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
);
INSERT INTO ice.spike.scd2_probe VALUES
    ('k1', 'v1', TIMESTAMP '2026-01-01 00:00:00', NULL, true);

-- PHASE needs-change
SELECT CASE
    WHEN count(*) = 1 THEN 'needs-change=1'
    ELSE 'needs-change=0'
END AS marker
FROM ice.spike.scd2_probe
WHERE business_key = 'k1'
  AND is_current = true
  AND payload <> 'v2';

-- PHASE change
BEGIN TRANSACTION;
UPDATE ice.spike.scd2_probe
SET is_current = false,
    valid_to = TIMESTAMP '2026-01-02 00:00:00'
WHERE business_key = 'k1'
  AND is_current = true;
INSERT INTO ice.spike.scd2_probe VALUES
    ('k1', 'v2', TIMESTAMP '2026-01-02 00:00:00', NULL, true);
COMMIT;

-- PHASE fail
BEGIN TRANSACTION;
UPDATE ice.spike.scd2_probe
SET payload = 'rolled-back'
WHERE business_key = 'k1'
  AND is_current = true;
SELECT CAST('forced-statement-failure' AS INTEGER);
COMMIT;

-- PHASE current-ok
SELECT CASE
    WHEN (
        SELECT count(*)
        FROM ice.spike.scd2_probe
        WHERE business_key = 'k1'
          AND is_current = true
          AND payload = 'v2'
    ) = 1
     AND (
        SELECT count(*)
        FROM ice.spike.scd2_probe
        WHERE payload = 'rolled-back'
    ) = 0
    THEN 'current-row: pass'
    ELSE 'current-row: fail'
END AS assertion;

-- PHASE read
SELECT CASE
    WHEN (
        SELECT count(*)
        FROM ice.spike.scd2_probe
        WHERE business_key = 'k1'
          AND is_current = true
          AND payload = 'v2'
    ) = 1
    THEN 'read-current: pass'
    ELSE 'read-current: fail'
END AS assertion;
