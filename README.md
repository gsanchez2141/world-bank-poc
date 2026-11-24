# world-bank-poc

A local World Bank lakehouse. The agreed design is in [ARCHITECTURE.md](ARCHITECTURE.md). AWS calls use the local emulator at `https://127.0.0.1:9297`.

## Platform

Run `make platform` to start the stack and apply Terraform. `make down` stops it while retaining the Postgres volume. `make test-spike` runs the Iceberg and dbt spikes. `make run` and `make demo` become available when the pipeline DAG is added.

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

`make test-unit` runs isolated unit tests. `make test` also runs fixture integration tests against the provisioned local emulator and warehouse.

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

`make test-unit` runs ingest and stage offline unit tests. `make test` additionally runs both integrations against the local emulator, warehouse and Iceberg REST catalog.
