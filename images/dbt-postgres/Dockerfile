FROM python:3.12-slim

RUN pip install --no-cache-dir dbt-core==1.12.7 dbt-postgres==1.11.0 pyarrow==18.1.0 \
    && dbt --version

ENV DBT_PROFILES_DIR=/usr/app \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DO_NOT_TRACK=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /usr/app
COPY images/ingest-world-bank/requirements.txt /tmp/ingest-requirements.txt
RUN pip install --no-cache-dir -r /tmp/ingest-requirements.txt
COPY images/ingest-world-bank/world_bank_ingest ./world_bank_ingest
COPY images/ingest-world-bank/world_bank_ingest/pipeline_run.sql ./fixture/pipeline_run.sql
COPY images/consumer-postgres/world_bank_consumer/staging.sql ./fixture/staging.sql
COPY images/dbt-postgres/dbt_project.yml images/dbt-postgres/profiles.yml ./
COPY images/dbt-postgres/models ./models
COPY images/dbt-postgres/curated ./curated
COPY config/pipeline.yml ./config/pipeline.yml
COPY images/dbt-postgres/world_bank_curated ./world_bank_curated
COPY images/dbt-postgres/world_bank_analytics ./world_bank_analytics
COPY images/dbt-postgres/analytics_tests ./analytics_tests
COPY images/dbt-postgres/tests ./curated_tests
ENTRYPOINT ["python", "-m", "world_bank_curated"]
