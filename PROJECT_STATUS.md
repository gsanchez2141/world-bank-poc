# Project status

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` is the clean-checkout command. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/local-platform`. Not `main`. Nothing is committed and nothing is pushed.

Current wave: platform. Auditor scopes for infra, email, Iceberg, and dbt are merge-ready. `make test` is reserved for later unit and integration tests. `make test-spike` runs the Iceberg and dbt spikes. The user is reviewing this MR before the ingest MR starts.

`make platform` with no email file exits 0. `make test` exits 0, including a recreated emulator whose Postgres volume still pointed at a missing Iceberg metadata object. `make demo` is not the pipeline yet. It exits non-zero and prints `pipeline DAG is not in this MR`.

DuckDB 2.0.1 commits an SCD2 close and insert as two snapshots, overwrite then append. dbt 2.0.12 passed `run`, `test`, `compile --static-analysis baseline`, and `docs generate`.

Wave list:

| Wave | Type | Status |
|---|---|---|
| platform | feature | done, awaiting user review |
| ingest | feature | pending |
| stage | feature | pending |
| consumer | feature | pending |
| curated | feature | pending |
| world_bank_pipeline | feature | pending |
| world_bank_backfill | feature | pending |
| analytic outputs | feature | pending |
| data quality | feature | pending |
| production note | feature | pending |

Open decisions: none. The email step is a planned user action, not an open design question.

Blockers: none.
