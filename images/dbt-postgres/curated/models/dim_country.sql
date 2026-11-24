{{ config(unique_key='country_sk') }}
select {{ surrogate('country_iso3') }} as country_sk, country_iso3, country_name,
       {{ surrogate('income_level_id') }} as income_level_sk
from ({{ latest_staging('country', ['country_iso3']) }}) latest
