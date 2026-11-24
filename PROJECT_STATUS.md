# Project status

Cold start, in this order: [ARCHITECTURE.md](ARCHITECTURE.md), this file, `PLAN.md`, `IMPLEMENTATION_CONTRACT.md`. `ARCHITECTURE.md` is the agreed proposal and the architecture. The other three do not restate it. A session that has not read `ARCHITECTURE.md` cannot continue.

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` becomes the clean-checkout command in the production-note MR. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/world-bank-pipeline`, from `main` at `54e3542`. Platform, ingest, stage, consumer, and curated are merged.

Current wave: the `world_bank_pipeline` Airflow DAG. Orchestrate the reviewed job images with one writer, run gating and resume, visible failure, and `make run`. Contract Acceptance Gate: **Accepted for Machinist** for [Only writer DAG and Airflow run command](IMPLEMENTATION_CONTRACT.md#wave-world_bank_pipeline--only-writer-dag-and-airflow-run-command). Local DockerOperator launch and authenticated REST status probes passed. The Auditor passed focused rereview after correction attempt 1 made the trigger report its IDs before submission and made the gate reject non-object input before opening Postgres.

Validation: the full `make test` passed, including the disposable real Airflow/Docker fixture. After the trigger and gate corrections, `make test pipeline` passed seven offline tests and the same real fixture. It checks one active writer, exact DagRun status, gate-only XCom, failed ingestion and one SNS acknowledgement, abandonment, and same-ID resume without rerunning completed jobs. Compose configuration, selector dry runs, and whitespace checks passed.

Wave list:

| Wave | Type | Status |
|---|---|---|
| platform | feature | done, merged to main |
| ingest | feature | done, merged to main |
| stage | feature | done, merged to main |
| consumer | feature | done, merged to main |
| curated | feature | done, merged to main |
| world_bank_pipeline | feature | implemented and audited, awaiting user review |
| world_bank_backfill | feature | pending |
| analytic outputs | feature | pending |
| data quality | feature | pending |
| production note | feature | pending |

Open decisions: none. See `QUESTIONS_AND_DECISIONS.md`.

Implementation blockers: none. User review is the checkpoint before the backfill DAG.
