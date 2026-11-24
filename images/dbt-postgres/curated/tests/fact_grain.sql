select country_sk, indicator_sk, year_sk
from {{ ref('fact_indicator_year') }}
group by country_sk, indicator_sk, year_sk
having count(*) > 1
