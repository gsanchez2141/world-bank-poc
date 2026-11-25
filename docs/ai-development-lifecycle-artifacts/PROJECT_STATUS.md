# Project status

Cold start, in this order: [ARCHITECTURE.md](../../ARCHITECTURE.md), this file, `PLAN.md`, `IMPLEMENTATION_CONTRACT.md`. `ARCHITECTURE.md` is the agreed proposal and the architecture. The other three do not restate it. A session that has not read `ARCHITECTURE.md` cannot continue.

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` becomes the clean-checkout command in the production-note MR. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/production-demo`, from `main` at `d08752c`. Data quality and earlier waves are merged.

Current state: the production design, local demo, missing-data scenario, stack reset, scheduled DAG pause default, and report samples are complete locally. The scoped commits are ready for user review.

Verification is recorded in [PROJECT_STATUS_HISTORY.md](PROJECT_STATUS_HISTORY.md#production-design-and-local-demo).

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
| analytic outputs | feature | done, merged to main |
| data quality | feature | done, merged to main |
| production note and full local demo | feature | complete locally, awaiting user review |

Open decisions: none. See `QUESTIONS_AND_DECISIONS.md`.

No implementation tasks are pending. On an existing local stack, `make demo` may stop during setup if the optional emulator alert secret already has a value. A fresh-stack demo was verified. `make destroy` was separately verified as a reset.
