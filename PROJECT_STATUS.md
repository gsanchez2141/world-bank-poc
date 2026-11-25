# Project status

Cold start, in this order: [ARCHITECTURE.md](ARCHITECTURE.md), this file, `PLAN.md`, `IMPLEMENTATION_CONTRACT.md`. `ARCHITECTURE.md` is the agreed proposal and the architecture. The other three do not restate it. A session that has not read `ARCHITECTURE.md` cannot continue.

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` becomes the clean-checkout command in the production-note MR. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/analytic-outputs`, from `main` at `9999d71`. The job images, writer DAG, and backfill DAG are merged.

Current wave: analytic outputs. Produce four Postgres reporting views and publish local CSV plus emulator CSV and Parquet exports. Contract Acceptance Gate: **Accepted for Machinist** for [Calculate the four Postgres reports](IMPLEMENTATION_CONTRACT.md#task-reporting-views--calculate-the-four-postgres-reports). The reporting SQL passed its disposable Postgres fixture and Auditor review. Contract Acceptance Gate: **Accepted for Machinist** for [Publish typed report files and resume after curated](IMPLEMENTATION_CONTRACT.md#task-reporting-export--publish-typed-report-files-and-resume-after-curated).

Validation: `make test analytics`, `make test pipeline`, and the full `make test` passed. The aggregate ran 37 offline and 10 local integration tests. The fixtures verified reporting calculations, typed exports and the local mirror, legacy upgrade, and a same-ID export-only retry after curated completion. The SQL and export scopes passed their separate Auditor reviews. Compose configuration, selector dry runs, and whitespace checks passed.

Wave list:

| Wave | Type | Status |
|---|---|---|
| platform | feature | done, merged to main |
| ingest | feature | done, merged to main |
| stage | feature | done, merged to main |
| consumer | feature | done, merged to main |
| curated | feature | done, merged to main |
| world_bank_pipeline | feature | done, merged to main |
| world_bank_backfill | feature | done, merged to main |
| analytic outputs | feature | implemented and audited, awaiting user review |
| data quality | feature | pending |
| production note | feature | pending |

Open decisions: none. See `QUESTIONS_AND_DECISIONS.md`.

Implementation blockers: none. User review is the checkpoint before data quality.
