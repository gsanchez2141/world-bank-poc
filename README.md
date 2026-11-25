# world-bank-poc

A local World Bank lakehouse. The agreed design is in [ARCHITECTURE.md](ARCHITECTURE.md). AWS calls use the local emulator at `https://127.0.0.1:9297`.

## Platform

Run `make platform` to start the stack and apply Terraform. `make down` stops it while retaining the Postgres volume. `make test-spike` runs the Iceberg and dbt spikes. `make run` triggers the writer DAG. `make demo` becomes available in the production-note wave.

## Ingest

With the platform running, build and run the standalone image:

```sh
docker compose -f compose.infra.yaml -f compose.jobs.yaml build lakehouse-ingest-world-bank
export WORLD_BANK_ALERT_TOPIC_ARN="$(./scripts/emulator-env.sh terraform -chdir=infra/terraform output -raw topic_arn)"
docker compose -f compose.infra.yaml -f compose.jobs.yaml run --rm lakehouse-ingest-world-bank --ingestion-id local-extract --countries BRA --indicators SP.POP.TOTL --start-year 2020 --end-year 2021
```

The defaults are in `config/pipeline.yml`. A failed run exits nonzero and can be retried with the same ID and scope. Each retry preserves the earlier attempt. Replay an already completed extract without contacting the API:

```sh
docker compose -f compose.infra.yaml -f compose.jobs.yaml run --rm lakehouse-ingest-world-bank --ingestion-id local-replay --mode replay --source-ingestion-id local-extract
```

Read its run log with:

```sh
./scripts/emulator-env.sh aws logs filter-log-events --log-group-name /world-bank/lakehouse-ingest-world-bank --log-stream-names local-extract
```


## Stage

After a successful extract or replay, use its ingestion ID to create Iceberg versions:

```sh
docker compose -f compose.infra.yaml -f compose.jobs.yaml build lakehouse-stage-world-bank
export WORLD_BANK_ALERT_TOPIC_ARN="$(./scripts/emulator-env.sh terraform -chdir=infra/terraform output -raw topic_arn)"
docker compose -f compose.infra.yaml -f compose.jobs.yaml run --rm lakehouse-stage-world-bank --ingestion-id local-extract
```

Repeat the same command to resume an interrupted stage run. Inspect its run log with:

```sh
./scripts/emulator-env.sh aws logs filter-log-events --log-group-name /world-bank/lakehouse-stage-world-bank --log-stream-names local-extract
```


## Consumer

A completed stage commit can be loaded into Postgres staging with:

```sh
docker compose -f compose.infra.yaml -f compose.jobs.yaml build lakehouse-consumer-postgres
export WORLD_BANK_ALERT_TOPIC_ARN="$(./scripts/emulator-env.sh terraform -chdir=infra/terraform output -raw topic_arn)"
docker compose -f compose.infra.yaml -f compose.jobs.yaml run --rm --no-deps lakehouse-consumer-postgres --ingestion-id local-extract
```

Retry the same command to resume an unfinished staging load. A completed load returns immediately. Use a distinct reprocess invocation to reload a commit. Read its logs with:

```sh
./scripts/emulator-env.sh aws logs filter-log-events --log-group-name /world-bank/lakehouse-consumer-postgres --log-stream-names local-extract
```

`make test` runs all installed unit and integration suites. Select individual suites with `make test ingestion`, `make test stage`, `make test consumer`, `make test curated`, `make test pipeline`, `make test backfill`, or `make test analytics`. Multiple selectors may be combined. Each selection builds its image and runs its unit and integration tests. `make test-unit` runs all installed offline unit suites, and `make test-spike` runs the platform spikes. Integration suites require the local emulator, warehouse and Iceberg REST catalog.

## Curated star

After a completed consumer load, build and publish the curated star and native dbt catalog:

```sh
docker compose -f compose.infra.yaml -f compose.jobs.yaml build dbt-postgres
export WORLD_BANK_ALERT_TOPIC_ARN="$(./scripts/emulator-env.sh terraform -chdir=infra/terraform output -raw topic_arn)"
docker compose -f compose.infra.yaml -f compose.jobs.yaml run --rm --no-deps dbt-postgres --ingestion-id local-extract
```

A failed run can be retried with the same ID without rerunning the consumer. The native manifest and catalog are written under `s3://lakehouse-curated/world-bank/dbt_catalog/date={stage-day}/ingestion_id={id}/`. Read the job log with:

```sh
./scripts/emulator-env.sh aws logs filter-log-events --log-group-name /world-bank/dbt-postgres --log-stream-names local-extract
```

`make test curated` runs the curated unit and integration suites. `make test` runs all four installed suites. `make test-unit` runs their offline units.


## Writer pipeline

`make platform` builds the four local job images, starts Airflow and the local services, and applies the dev Terraform resources. The writer DAG is unpaused and manual by default. Set the trusted scheduler environment variable `WORLD_BANK_PIPELINE_SCHEDULE` to a cron expression or Airflow preset to enable scheduling. It runs at most one writer DagRun at a time.

Trigger and wait for a new run with `make run`. The command reports the Airflow DagRun ID, ingestion ID and terminal state. It uses the local Airflow account and `http://127.0.0.1:8080`. A timeout leaves the DagRun available for inspection in Airflow. To select a scope or resume a run:

```sh
WORLD_BANK_RUN_CONF='{"ingestion_id":"local-bra","countries":["BRA"],"indicators":["SP.POP.TOTL"],"start_year":2020,"end_year":2021}' make run
WORLD_BANK_RUN_CONF='{"ingestion_id":"local-bra"}' make run
```

Resume preserves the ledger scope. A new extract is blocked while an earlier nonabandoned run is unfinished. To preserve its evidence and explicitly proceed with a new extract, supply `"abandon_previous":true` with the new ingestion ID. An abandoned ID cannot resume.

`extract` and `replay` run the raw and stage jobs before consumer and curated. `replay` requires a different completed extract ID in `source_ingestion_id`. `reprocess_commit` requires a different stage-complete extract or replay ID and runs consumer and curated. `rebuild` restores current Iceberg rows into empty canonical staging and curated tables.

The scheduler uses the local Docker socket, the emulator network namespace and a read-only emulator credential mount for job containers. Colima defaults to Docker group 991. Set `WORLD_BANK_DOCKER_GID` if the local daemon socket uses another group, or `WORLD_BANK_DOCKER_SOCKET` if its engine-host socket path differs. Read failure and alert events with:

```sh
./scripts/emulator-env.sh aws logs filter-log-events --log-group-name /world-bank/airflow --log-stream-names local-bra
```


## Backfill

Trigger the manual `world_bank_backfill` DAG from the Airflow UI or its authenticated local REST API. The parent triggers one writer child, logs its exact Airflow run ID and ingestion ID, and succeeds only when that child succeeds. A parent task retry reattaches to the same child. A new parent run can use the same ingestion ID to resume an unfinished writer.

For example, set the local Airflow endpoint and submit a narrowed extract:

```sh
AIRFLOW_URL=http://127.0.0.1:8080
curl -u airflow:airflow -H 'Content-Type: application/json' \
  -d '{"conf":{"mode":"extract","ingestion_id":"short-bra","countries":["BRA"],"indicators":["SP.POP.TOTL"],"start_year":2021,"end_year":2021}}' \
  "$AIRFLOW_URL/api/v1/dags/world_bank_backfill/dagRuns"
```

The same endpoint accepts the following `conf` objects for saved data and recovery:

```json
{"mode":"replay","ingestion_id":"replay-bra","source_ingestion_id":"short-bra"}
{"mode":"reprocess_commit","ingestion_id":"reprocess-bra","source_ingestion_id":"short-bra"}
{"mode":"rebuild","ingestion_id":"restore-warehouse"}
```

Rebuild requires the four canonical `stg` and five `curated` tables to be absent or empty. It preserves the stage Iceberg tables and restores their pinned current rows into the warehouse. `make run` continues to trigger the writer directly. `make test backfill` runs the backfill tests.


## Reporting exports

A successful writer run publishes CSV and Parquet for the four reports under `s3://lakehouse-curated/world-bank/reporting/date={stage-day}/ingestion_id={id}/`. The current successful CSV mirror is written to:

- `exports/vw_missing_or_incomplete.csv`
- `exports/vw_yoy_change.csv`
- `exports/vw_latest_year_comparison.csv`
- `exports/vw_income_group_year.csv`

The files are available after `export_status` becomes `complete`. If export fails, resume the same writer ingestion ID with `make run`; already completed upstream work is reused. `make test analytics` runs the reporting view and artifact suites.
