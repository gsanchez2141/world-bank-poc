{{ config(unique_key='year_sk') }}
with years as (
 select year from ({{ latest_staging('observation', ['country_iso3', 'indicator_code', 'year']) }}) latest
 union
 select generate_series(start_year, end_year) as year
 from {{ source('meta', 'pipeline_run') }}
 where consumer_status = 'complete' and mode in ('extract', 'replay')
)
select {{ surrogate('year') }} as year_sk, year from years
