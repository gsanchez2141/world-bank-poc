# world-bank-poc

A local World Bank lakehouse. The agreed design is in [ARCHITECTURE.md](ARCHITECTURE.md). AWS calls use the local emulator at `https://127.0.0.1:9297`.

## Run the local demo

* `make demo` — Run the full demo from a clean checkout. It starts the platform, runs the default extract for 9 countries and 4 indicators from 2018–2024 (252 expected observations), then runs the Brazil 2021 population backfill. After both runs succeed, it prints the four CSV paths. See [Reporting exports](#reporting-exports).

* `make demo missing` — Run an isolated fixture that omits Brazil’s 2021 GDP and GDP per capita from a later extract. The demo writes their retained fixture values and the `MISSING_FROM_LATEST_EXTRACT` reason to `exports/vw_missing_or_incomplete_demo.csv`.
* `make platform` — Start the local services and apply Terraform without triggering the pipeline.
* `make run` — Trigger the pipeline on the running platform and wait for it to finish.
* `make test` — Run all installed test suites. Use a selector such as `make test backfill` to run one suite.
* `make down` — Stop and remove the containers while keeping the Postgres and Airflow log volumes. Emulator objects and logs live until its container stops.
* `make destroy` — Remove the containers, networks, and volumes for the local stack, including warehouse data, the Iceberg catalog, and Airflow logs. Saved CSVs stay in `exports/`. Run `make demo` to start fresh.

The host needs Docker running with Compose v2, Make, Python 3, the AWS CLI, Terraform compatible with the tracked provider lock, and curl. The demo wrapper automatically looks in `~/.local/bin` and on your existing `PATH` for Terraform. OpenSSL is needed when supplying the optional alert-email key. The default extract needs outbound HTTPS to the public World Bank API. The local AWS commands use the emulator at `https://127.0.0.1:9297` and require no AWS account or API key.

The tracked encrypted alert address is optional. If `secrets/email.key` is absent, setup skips its subscription and continues. Keep a supplied key private and outside version control.

The demo prints the Airflow and ingestion IDs. Inspect a failed task in the Airflow UI at `http://127.0.0.1:8080` using the local `airflow` / `airflow` login, or read its events with the command in [Airflow DAG](#airflow-dag). Resume a failed pipeline run with the same ingestion ID after correcting the cause.

## Assumptions and key decisions

The default scope in `config/pipeline.yml` defines which country–indicator–year combinations we expect to see. For example, Brazil’s population in 2021 is one expected observation. The API can return a number, return an explicit null, or omit the observation entirely. Only a numeric zero in the source represents zero.

1. **Raw preserves the evidence.** We save the full API response and carry its object key and record index alongside the observation. An analyst can trace the curated value back to the exact source record.
2. **Iceberg keeps the revision history.** If the World Bank revises Brazil’s 2021 population, stage closes the previous version and opens a new one. Receiving the same source fields again creates no new version.
3. **Postgres stores all countries’ observations in a single fact table.** Each row holds the latest observation for one combination of country, indicator, and year. The warehouse loads the recorded Iceberg changes, and dbt updates the matching row when a revision arrives. Replaying unchanged data creates no duplicate rows.

The `vw_missing_or_incomplete` report explains gaps. If the API has never returned an expected observation, the report shows `NEVER_RETURNED` and there's no row in the fact. An explicit null is stored as SQL `NULL` and reported as `SOURCE_NULL`. If a later extract requests the same observation and omits it, the fact retains its last value and the report flags `MISSING_FROM_LATEST_EXTRACT`.

## Run and test individual pipeline layers

The sections below show how to run each pipeline layer individually, starting with the local services they share. Use these commands to inspect intermediate outputs or troubleshoot a specific step.

* `make test` runs all installed unit and integration suites.

* Select specific suites with `make test ingestion`, `make test stage`, `make test consumer`, `make test curated`, `make test pipeline`, `make test backfill`, `make test analytics`, or `make test dq`. Each selector runs the unit and integration tests for that suite. Combine selectors in one command, for example `make test stage curated`.

* `make test-unit` runs all installed offline unit suites.

* `make test-spike` runs the platform spikes.

Integration suites require the local emulator, warehouse, and Iceberg REST catalog.

## Platform

Run `make platform` to start the stack and apply Terraform without triggering a pipeline. `make run` triggers the writer DAG, and `make test-spike` runs the Iceberg and dbt spikes. The pinned dbt runtime and catalog probe are described in [dbt runtime](docs/dbt-runtime.md).

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


## Airflow DAG

`make platform` builds the four local job images, starts Airflow and the local services, and applies the dev Terraform resources. The `world_bank_pipeline` DAG is manual and unpaused by default. Set the scheduler environment variable `WORLD_BANK_PIPELINE_SCHEDULE` to a cron expression or Airflow preset to enable scheduling. A newly registered DAG with a schedule starts paused. Unpause it in Airflow before scheduled or manual runs can execute. An existing DAG keeps its current pause state when its schedule changes. The DAG runs at most one pipeline execution at a time.

Trigger and wait for a new run with `make run`. The command reports the Airflow DagRun ID, ingestion ID and terminal state. It uses the local Airflow account and `http://127.0.0.1:8080`. A timeout leaves the DagRun available for inspection in Airflow.

```sh
# Full run for all configured countries, indicators, and years
make run

# Run only Brazil population for 2020–2021
WORLD_BANK_RUN_CONF='{"ingestion_id":"local-bra","countries":["BRA"],"indicators":["SP.POP.TOTL"],"start_year":2020,"end_year":2021}' make run

# Resume the Brazil run if it failed
WORLD_BANK_RUN_CONF='{"ingestion_id":"local-bra"}' make run
```

Resuming a run uses the original request scope saved in the pipeline run status table (`meta.pipeline_run`). A new extract is blocked while an earlier nonabandoned run is unfinished. To preserve its evidence and explicitly proceed with a new extract, supply `"abandon_previous":true` with the new ingestion ID. An abandoned ID cannot resume.

`extract` and `replay` run the raw and stage jobs before consumer and curated. `replay` requires a different completed extract ID in `source_ingestion_id`. `reprocess_commit` requires a different stage-complete extract or replay ID and runs consumer and curated. `rebuild` restores current Iceberg rows into empty canonical staging and curated tables.

The scheduler uses the local Docker socket, the emulator network namespace and a read-only emulator credential mount for job containers. Colima defaults to Docker group 991. Set `WORLD_BANK_DOCKER_GID` if the local daemon socket uses another group, or `WORLD_BANK_DOCKER_SOCKET` if its engine-host socket path differs. Read failure and alert events with:

```sh
./scripts/emulator-env.sh aws logs filter-log-events --log-group-name /world-bank/airflow --log-stream-names local-bra
```


## Airflow DAG Backfill

Trigger the manual `world_bank_backfill` DAG from the Airflow UI or its authenticated local REST API. The backfill DAG starts one `world_bank_pipeline` run, logs its Airflow run ID and ingestion ID, and waits for it to succeed. If the backfill task retries, it resumes waiting for the same run. A new backfill run can use the same ingestion ID to resume an unfinished pipeline execution.

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


## Data quality

The pipeline checks curated data before export.

* **Population values.** When a population value is present, it must be positive.

* **GDP per capita consistency.** When GDP, population, and GDP per capita are comparable, GDP divided by population must agree with reported GDP per capita within the tolerances in `config/pipeline.yml`.

* **Missing observations.** If an extract omits a previously received observation within its requested scope, the pipeline logs a warning and keeps the last observation in the fact table.

* **Warning count.** The pipeline sends a local SNS notification when the number of omission warnings increases over the comparable baseline by more than the configured threshold.

A failed quality check leaves the writer run failed and export incomplete. Inspect `meta.dq_result` and the run log, correct the underlying data, and resume the same ingestion ID. The writer reuses completed upstream work and runs quality before export. `make test dq` runs the quality checks and its private writer fixture.

## Reporting exports

The [sample CSVs](docs/sample-outputs/) include three reports from a local run using World Bank data. The [missing-observations sample](docs/sample-outputs/vw_missing_or_incomplete_demo.csv) comes from `make demo missing`. The fixture deliberately omits Brazil’s 2021 GDP and GDP per capita from a later extract to verify that `vw_missing_or_incomplete` flags both rows as `MISSING_FROM_LATEST_EXTRACT` and shows their last known values.

A successful writer run publishes CSV and Parquet for the four reports under `s3://lakehouse-curated/world-bank/reporting/date={stage-day}/ingestion_id={id}/`. The current successful CSV mirror is written to:

- `exports/vw_missing_or_incomplete.csv`
- `exports/vw_yoy_change.csv`
- `exports/vw_latest_year_comparison.csv`
- `exports/vw_income_group_year.csv`

The files are available after `export_status` becomes `complete`. If export fails, resume the same writer ingestion ID with `make run`; already completed upstream work is reused. `make test analytics` runs the reporting view and artifact suites.

## Operating in production

* **Scheduling and incremental refreshes.** Schedule the Airflow DAG in Amazon MWAA at a cadence agreed with report consumers. Recheck a configurable window of reporting years and load only new or changed observations from the resulting Iceberg snapshots.

* **Late and corrected data.** Recheck older years periodically or through a scoped backfill. Iceberg keeps each revision, and Snowflake updates the current observation by country, indicator, and year. Preserve explicit source nulls and flag observations omitted from a later extract.

* **Monitoring and data quality.** Track run status, report freshness, and quality findings in CloudWatch. Send SNS alerts for failures or spikes in omission warnings. Block publication when a hard quality check fails.

* **Backfills and replay.** Scope backfills by country, indicator, and year. Keep successful raw responses and committed snapshot IDs so a run can replay saved input or reprocess a warehouse load.

* **AWS and Snowflake.** Use ECS or Glue for ingestion and AWS Glue or EMR Spark for staging. Store Iceberg tables on S3 with Glue Data Catalog. Use dbt on Snowflake for the curated model and reporting views.

See the [production design](production-design.md) for the service mapping, operational details, and diagrams.

## Design tradeoff and AI usage

The demo uses data from the World Bank for four indicators across nine countries from 2018 through 2024. At that scope, a scheduled job could load the data directly into a relational database. This proof of concept uses separate raw, Iceberg stage, and warehouse layers to demonstrate saved source responses, recoverable runs, provenance, data quality, and queryable missingness. The proposed AWS and Snowflake mapping is in [Production design](production-design.md).

* **One pipeline run at a time.** Extracts and backfills share the same Iceberg tables and warehouse. Airflow runs them sequentially to avoid overlapping writes and simplify recovery. A backfill waits while another pipeline run is active.

* **Revision history in Iceberg.** Iceberg stage keeps revisions as SCD Type 2 versions. Curated Postgres uses Type 1 behavior for revisions, updating the existing observation when a new value arrives. This keeps everyday reporting simple. To compare earlier revisions of an observation, query Iceberg.

* **Block exports on hard quality failures.** The local pipeline stops before publishing when a blocking check fails. This keeps invalid results out of the reports and delays publication until the issue is fixed and the run resumes. Missing observations can still be published with warnings.

  In production, the SLA could define thresholds for the percentage of rows that fail validation in a run. For example:

  * **Up to 5%.** Publish the valid rows and quarantine the failed rows in S3 or send them to a dead-letter queue for reprocessing.

  * **Over 5% and up to 20%.** Raise an alert and continue publishing valid rows. Keep the failed rows available for reprocessing.

  * **Over 20%.** Raise an alert and stop the pipeline before publication.

Codex assisted with implementation, review, tests, and documentation under the developer's direction. The developer chose the [architecture](ARCHITECTURE.md) and reviewed the milestones and pull requests.

The plan, decisions, implementation contract, project status, and their histories are indexed in the [AI development lifecycle documentation](docs/ai-development-lifecycle-artifacts/documentation.md).
