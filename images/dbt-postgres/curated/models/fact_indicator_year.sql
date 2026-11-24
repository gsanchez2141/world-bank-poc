{{ config(unique_key=['country_sk', 'indicator_sk', 'year_sk']) }}
with observation as (
 {{ latest_staging('observation', ['country_iso3', 'indicator_code', 'year']) }}
)
select {{ surrogate('o.country_iso3') }} as country_sk,
       {{ surrogate('o.indicator_code') }} as indicator_sk,
       {{ surrogate('o.year') }} as year_sk,
       o.value, o.raw_response_key, o.raw_record_index, o.response_sha256,
       o.raw_object_key, o.source_url, o.api_page, o.raw_ingested_at, o.ingestion_id
from observation o
left join {{ ref('dim_country') }} c on c.country_iso3 = o.country_iso3
left join {{ ref('dim_indicator') }} i on i.indicator_code = o.indicator_code
left join {{ ref('dim_year') }} y on y.year = o.year
