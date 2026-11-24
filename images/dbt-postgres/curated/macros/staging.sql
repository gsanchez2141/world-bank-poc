{% macro surrogate(key) -%}
('x' || substr(md5(cast({{ key }} as text)), 1, 16))::bit(64)::bigint
{%- endmacro %}

{% macro latest_staging(table, keys) -%}
select * from (
  select s.*, row_number() over (
    partition by {% for key in keys %}s.{{ key }}{% if not loop.last %}, {% endif %}{% endfor %}
    order by s.version desc, s.stage_ingested_at desc, s.stage_run_id desc, s.consumer_run_id desc
  ) as selected_rank
  from {{ source('stg', table) }} s
  join {{ source('meta', 'pipeline_run') }} r on r.ingestion_id = s.consumer_run_id
  where r.consumer_status = 'complete' and s.change_kind = 'append' and s.is_current = true
) ranked where selected_rank = 1
{%- endmacro %}
