CREATE OR REPLACE VIEW curated.vw_missing_or_incomplete AS
WITH facts AS (
    SELECT c.country_iso3, i.indicator_code, y.year, f.value, f.country_sk
    FROM curated.fact_indicator_year f
    JOIN curated.dim_country c ON c.country_sk = f.country_sk
    JOIN curated.dim_indicator i ON i.indicator_sk = f.indicator_sk
    JOIN curated.dim_year y ON y.year_sk = f.year_sk
),
classified AS (
    SELECT e.country_iso3, e.indicator_code, e.year, f.value,
           CASE
               WHEN f.country_sk IS NULL THEN 'NEVER_RETURNED'
               WHEN warning.ingestion_id IS NOT NULL THEN 'MISSING_FROM_LATEST_EXTRACT'
               WHEN f.value IS NULL THEN 'SOURCE_NULL'
           END AS reason
    FROM meta.expected_key e
    LEFT JOIN facts f USING (country_iso3, indicator_code, year)
    LEFT JOIN LATERAL (
        SELECT r.ingestion_id
        FROM meta.pipeline_run r
        WHERE r.mode = 'extract'
          AND r.raw_status = 'complete'
          AND e.country_iso3 = ANY(r.request_countries)
          AND e.indicator_code = ANY(r.request_indicators)
          AND e.year BETWEEN r.start_year AND r.end_year
        ORDER BY r.raw_ingested_at DESC NULLS LAST, r.ingestion_id DESC
        LIMIT 1
    ) latest ON true
    LEFT JOIN meta.dq_result warning
      ON warning.ingestion_id = latest.ingestion_id
     AND warning.check_name = 'observation_disappeared'
     AND warning.severity = 'warning'
     AND warning.business_key = jsonb_build_object(
         'country_iso3', e.country_iso3, 'indicator_code', e.indicator_code, 'year', e.year)
)
SELECT country_iso3, indicator_code, year, value, reason
FROM classified
WHERE reason IS NOT NULL;

CREATE OR REPLACE VIEW curated.vw_yoy_change AS
WITH facts AS (
    SELECT c.country_iso3, i.indicator_code, y.year, f.value
    FROM curated.fact_indicator_year f
    JOIN curated.dim_country c ON c.country_sk = f.country_sk
    JOIN curated.dim_indicator i ON i.indicator_sk = f.indicator_sk
    JOIN curated.dim_year y ON y.year_sk = f.year_sk
),
countries AS (SELECT DISTINCT country_iso3 FROM meta.expected_key),
indicators AS (SELECT DISTINCT indicator_code FROM meta.expected_key),
years AS (
    SELECT generate_series(min(year), max(year))::integer AS year
    FROM curated.dim_year
),
spine AS (
    SELECT c.country_iso3, i.indicator_code, y.year
    FROM countries c CROSS JOIN indicators i CROSS JOIN years y
),
comparison AS (
    SELECT s.country_iso3, s.indicator_code, s.year, current.value, prior.value AS prior_value
    FROM spine s
    LEFT JOIN facts current ON current.country_iso3 = s.country_iso3
        AND current.indicator_code = s.indicator_code AND current.year = s.year
    LEFT JOIN facts prior ON prior.country_iso3 = s.country_iso3
        AND prior.indicator_code = s.indicator_code AND prior.year = s.year - 1
)
SELECT country_iso3, indicator_code, year, value, prior_value,
       CASE WHEN value IS NOT NULL AND prior_value IS NOT NULL AND prior_value <> 0
            THEN (100 * (value - prior_value) / prior_value)::numeric(38,12)
       END AS yoy_change_pct,
       CASE WHEN value IS NULL THEN 'CURRENT_NULL'
            WHEN prior_value IS NULL THEN 'PRIOR_YEAR_MISSING'
            WHEN prior_value = 0 THEN 'PRIOR_YEAR_ZERO'
       END AS reason
FROM comparison;

CREATE OR REPLACE VIEW curated.vw_latest_year_comparison AS
WITH countries AS (SELECT DISTINCT country_iso3 FROM meta.expected_key),
indicators AS (SELECT DISTINCT indicator_code FROM meta.expected_key),
facts AS (
    SELECT c.country_iso3, i.indicator_code, y.year, f.value
    FROM curated.fact_indicator_year f
    JOIN curated.dim_country c ON c.country_sk = f.country_sk
    JOIN curated.dim_indicator i ON i.indicator_sk = f.indicator_sk
    JOIN curated.dim_year y ON y.year_sk = f.year_sk
),
latest AS (
    SELECT f.indicator_code, max(f.year)::integer AS year
    FROM facts f
    JOIN countries c USING (country_iso3)
    JOIN indicators i USING (indicator_code)
    WHERE f.value IS NOT NULL
    GROUP BY f.indicator_code
)
SELECT c.country_iso3, l.indicator_code, l.year, f.value
FROM latest l
CROSS JOIN countries c
LEFT JOIN facts f ON f.country_iso3 = c.country_iso3
    AND f.indicator_code = l.indicator_code AND f.year = l.year;

CREATE OR REPLACE VIEW curated.vw_income_group_year AS
WITH countries AS (
    SELECT DISTINCT country_iso3 FROM meta.expected_key
),
years AS (
    SELECT DISTINCT year FROM meta.expected_key
),
indicators AS (
    SELECT DISTINCT indicator_code FROM meta.expected_key
    WHERE indicator_code IN ('SP.POP.TOTL', 'NY.GDP.MKTP.CD', 'NY.GDP.PCAP.CD', 'SP.DYN.LE00.IN')
),
members AS (
    SELECT c.country_iso3, level.income_level_id, level.income_level_name
    FROM countries selected
    JOIN curated.dim_country c USING (country_iso3)
    JOIN curated.dim_income_level level ON level.income_level_sk = c.income_level_sk
),
facts AS (
    SELECT c.country_iso3, i.indicator_code, y.year, f.value
    FROM curated.fact_indicator_year f
    JOIN curated.dim_country c ON c.country_sk = f.country_sk
    JOIN curated.dim_indicator i ON i.indicator_sk = f.indicator_sk
    JOIN curated.dim_year y ON y.year_sk = f.year_sk
),
country_year AS (
    SELECT m.country_iso3, m.income_level_id, m.income_level_name, y.year,
           max(f.value) FILTER (WHERE f.indicator_code = 'SP.POP.TOTL') AS population,
           max(f.value) FILTER (WHERE f.indicator_code = 'NY.GDP.MKTP.CD') AS gdp,
           max(f.value) FILTER (WHERE f.indicator_code = 'SP.DYN.LE00.IN') AS life
    FROM members m CROSS JOIN years y
    LEFT JOIN facts f ON f.country_iso3 = m.country_iso3 AND f.year = y.year
    GROUP BY m.country_iso3, m.income_level_id, m.income_level_name, y.year
),
aggregated AS (
    SELECT income_level_id, income_level_name, year,
           count(*)::integer AS expected_country_count,
           count(population)::integer AS population_count,
           count(gdp)::integer AS gdp_count,
           count(*) FILTER (WHERE population IS NOT NULL AND gdp IS NOT NULL)::integer AS ratio_count,
           count(*) FILTER (WHERE population IS NOT NULL AND life IS NOT NULL)::integer AS life_count,
           sum(population) AS population_sum,
           sum(gdp) AS gdp_sum,
           sum(gdp) FILTER (WHERE population IS NOT NULL AND gdp IS NOT NULL) AS paired_gdp,
           sum(population) FILTER (WHERE population IS NOT NULL AND gdp IS NOT NULL) AS paired_population,
           sum(life * population) FILTER (WHERE population IS NOT NULL AND life IS NOT NULL) AS weighted_life,
           sum(population) FILTER (WHERE population IS NOT NULL AND life IS NOT NULL) AS life_population
    FROM country_year
    GROUP BY income_level_id, income_level_name, year
),
metrics AS (
    SELECT a.*, i.indicator_code,
           CASE i.indicator_code
               WHEN 'SP.POP.TOTL' THEN 'SUM'
               WHEN 'NY.GDP.MKTP.CD' THEN 'SUM'
               WHEN 'NY.GDP.PCAP.CD' THEN 'GDP_PER_CAPITA_RATIO'
               WHEN 'SP.DYN.LE00.IN' THEN 'POPULATION_WEIGHTED_MEAN'
           END AS aggregation_method,
           CASE i.indicator_code
               WHEN 'SP.POP.TOTL' THEN population_count
               WHEN 'NY.GDP.MKTP.CD' THEN gdp_count
               WHEN 'NY.GDP.PCAP.CD' THEN ratio_count
               WHEN 'SP.DYN.LE00.IN' THEN life_count
           END AS available_country_count,
           CASE i.indicator_code
               WHEN 'SP.POP.TOTL' THEN population_sum
               WHEN 'NY.GDP.MKTP.CD' THEN gdp_sum
               WHEN 'NY.GDP.PCAP.CD' THEN paired_gdp / NULLIF(paired_population, 0)
               WHEN 'SP.DYN.LE00.IN' THEN weighted_life / NULLIF(life_population, 0)
           END AS aggregate_value
    FROM aggregated a CROSS JOIN indicators i
)
SELECT income_level_id, income_level_name, year, indicator_code, aggregation_method,
       aggregate_value::numeric(38,12) AS value,
       expected_country_count, available_country_count,
       (100::numeric * available_country_count / expected_country_count)::numeric(38,12) AS coverage_pct
FROM metrics;
