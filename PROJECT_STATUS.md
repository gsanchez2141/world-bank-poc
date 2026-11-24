# Project status

Cold start, in this order: [ARCHITECTURE.md](ARCHITECTURE.md), this file, `PLAN.md`, `IMPLEMENTATION_CONTRACT.md`. `ARCHITECTURE.md` is the agreed proposal and the architecture. The other three do not restate it. A session that has not read `ARCHITECTURE.md` cannot continue.

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` becomes the clean-checkout command in the production-note MR. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/curated`, from `main` at `705efe0`. Platform, ingest, stage, and consumer are merged.

Current wave: curated tables. Build the dbt star from consumer staging, retain provenance, and publish catalog artifacts. The user selected dbt Core 1.12.7 with native model-level lineage and catalog. Contract Acceptance Gate: **Accepted for Machinist** for [Core Postgres star and native dbt catalog](IMPLEMENTATION_CONTRACT.md#wave-curated--core-postgres-star-and-native-dbt-catalog). The independent test suite selection task was accepted, implemented and passed Auditor review. All three named installed suites and the aggregate passed against the local stack. The curated selector now runs the real curated unit and integration suites.

Validation: `make test curated`, the full `make test` (21 unit tests and four local integrations), and both platform spikes passed. The Auditor passed the curated scope with no blockers. The curated wave awaits user review before DAG wiring.

Wave list:

| Wave | Type | Status |
|---|---|---|
| platform | feature | done, merged to main |
| ingest | feature | done, merged to main |
| stage | feature | done, merged to main |
| consumer | feature | done, merged to main |
| curated | feature | implemented and audited, awaiting user review |
| world_bank_pipeline | feature | pending |
| world_bank_backfill | feature | pending |
| analytic outputs | feature | pending |
| data quality | feature | pending |
| production note | feature | pending |

Open decisions: none. See `QUESTIONS_AND_DECISIONS.md`.

Curated capability: the dbt Core 1.12.7 and dbt-postgres 1.11.0 probe passed the model build and generated an error-free catalog. The Fusion 2.0.12 strict-analysis and catalog failures are recorded in ARCHITECTURE.md.
