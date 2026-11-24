# Project status history

Closed waves. The live status is `PROJECT_STATUS.md`.

## Platform

Branch `feat/local-platform`, merged to `main` as `9cf28c8`.

`make platform` with no email file exits 0. `make test` exits 0. `make test-spike` covers the Iceberg spike, including a recreated emulator whose Postgres volume still pointed at a missing Iceberg metadata object, and the dbt spike. `make down` keeps the Postgres volume. `make demo` exits non-zero and prints `pipeline DAG is not in this MR`.

DuckDB 2.0.1 commits an SCD2 close and insert as two snapshots, overwrite then append. dbt 2.0.12 passed `run`, `test`, `compile --static-analysis baseline`, and `docs generate`.

`scripts/emulator-env.sh` prepends `~/.local/bin` so Terraform resolves for that process only. That fix is in the merge.

Auditor scopes for infra, email, Iceberg, dbt, and the test-target rename were merge-ready.
