{{ config(unique_key='income_level_sk') }}
select {{ surrogate('income_level_id') }} as income_level_sk, income_level_id, income_level_name
from ({{ latest_staging('income_level', ['income_level_id']) }}) latest
