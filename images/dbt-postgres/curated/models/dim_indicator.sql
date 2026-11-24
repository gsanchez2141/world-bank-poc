{{ config(unique_key='indicator_sk') }}
select {{ surrogate('indicator_code') }} as indicator_sk, indicator_code, indicator_name
from ({{ latest_staging('indicator', ['indicator_code']) }}) latest
