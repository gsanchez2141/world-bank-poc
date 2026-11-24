# Project status

Cold start, in this order: [ARCHITECTURE.md](ARCHITECTURE.md), this file, `PLAN.md`, `IMPLEMENTATION_CONTRACT.md`. `ARCHITECTURE.md` is the agreed proposal and the architecture. The other three do not restate it. A session that has not read `ARCHITECTURE.md` cannot continue.

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` becomes the clean-checkout command in the production-note MR. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/consumer`, from `main` at `e921316`. Platform, ingest, and stage are merged.

Current wave: Postgres consumer. Load exact historical Iceberg snapshots into `stg`, retain provenance, and make a same-ID retry atomic. Contract Acceptance Gate: **Accepted for Machinist**. See [Exact Iceberg commit load into Postgres](IMPLEMENTATION_CONTRACT.md#wave-consumer--exact-iceberg-commit-load-into-postgres).

Validation: `make test` passed 15 offline unit tests and all three local integration scenarios. The consumer integration verified exact historical loads, lossless decimals, reprocess and zero-change completion, missing-snapshot rejection, and rollback of a failed replacement. Compose configuration, the spike target dry run, and whitespace checks passed. The Auditor passed the consumer scope on the first review.

Wave list:

| Wave | Type | Status |
|---|---|---|
| platform | feature | done, merged to main |
| ingest | feature | done, merged to main |
| stage | feature | done, merged to main |
| consumer | feature | implemented and audited, awaiting user review |
| curated | feature | pending |
| world_bank_pipeline | feature | pending |
| world_bank_backfill | feature | pending |
| analytic outputs | feature | pending |
| data quality | feature | pending |
| production note | feature | pending |

Open decisions: none. See `QUESTIONS_AND_DECISIONS.md`.

Implementation blockers: none. User review is the checkpoint before curated tables.
