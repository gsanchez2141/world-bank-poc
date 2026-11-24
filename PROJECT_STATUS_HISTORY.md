# Project status history

Closed waves. The live status is `PROJECT_STATUS.md`.

## Platform

Branch `feat/local-platform`, merged to `main` as `9cf28c8`.

`make platform` with no email file exits 0. `make test` exits 0. `make test-spike` covers the Iceberg spike, including a recreated emulator whose Postgres volume still pointed at a missing Iceberg metadata object, and the dbt spike. `make down` keeps the Postgres volume. `make demo` exits non-zero and prints `pipeline DAG is not in this MR`.

DuckDB 2.0.1 commits an SCD2 close and insert as two snapshots, overwrite then append. dbt 2.0.12 passed `run`, `test`, `compile --static-analysis baseline`, and `docs generate`.

`scripts/emulator-env.sh` prepends `~/.local/bin` so Terraform resolves for that process only. That fix is in the merge.

Auditor scopes for infra, email, Iceberg, dbt, and the test-target rename were merge-ready.

## Ingest

Branch `feat/ingest`.

Current wave: standalone World Bank raw ingestion. Implementation and review are complete. The image fetches or replays raw responses, publishes traceable batches and manifests, and reports run outcomes. The [ingest contract](IMPLEMENTATION_CONTRACT_HISTORY.md#wave-ingest--standalone-world-bank-raw-ingestion) was accepted, and the Auditor passed the focused rereview after correction attempt 1 restored independent SNS notification when CloudWatch stream creation fails. The user merged pull request #2 to `main` as `af1b325`.

Validation: `make test` built the image and passed six offline unit tests and the local Postgres/AWS emulator integration scenario covering failure, retry, repeat, replay, log retrieval, and SNS acknowledgement. Compose config, `make -n test-spike`, and whitespace checks passed. The tests used saved World Bank responses. Unit-only coverage was 53% and is report-only. The README documents operation and replaces the platform-only test instructions. No contract or decision changes were needed after implementation.
