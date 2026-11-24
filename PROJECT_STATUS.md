# Project status

Cold start, in this order: [ARCHITECTURE.md](ARCHITECTURE.md), this file, `PLAN.md`, `IMPLEMENTATION_CONTRACT.md`. `ARCHITECTURE.md` is the agreed proposal and the architecture. The other three do not restate it. A session that has not read `ARCHITECTURE.md` cannot continue.

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` becomes the clean-checkout command in the production-note MR. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/ingest`, from `main` at `9cf28c8`. Platform is merged.

Current wave: ingest. `IMPLEMENTATION_CONTRACT.md` points at `ARCHITECTURE.md` and no longer treats platform as the active wave. The ingest file plan is not written yet. Next step is that plan, then the machinist, then the auditor. The user reviews the MR before stage starts.

`make test` on `main` still prints `test suites are not in this MR`. This wave replaces that for the ingest suites. `make test-spike` stays the Iceberg and dbt spikes.

Wave list:

| Wave | Type | Status |
|---|---|---|
| platform | feature | done, merged to main |
| ingest | feature | in progress, contract not written |
| stage | feature | pending |
| consumer | feature | pending |
| curated | feature | pending |
| world_bank_pipeline | feature | pending |
| world_bank_backfill | feature | pending |
| analytic outputs | feature | pending |
| data quality | feature | pending |
| production note | feature | pending |

Open decisions: none. See `QUESTIONS_AND_DECISIONS.md`.

Blockers: none.
