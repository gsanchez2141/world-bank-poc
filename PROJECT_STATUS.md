# Project status

Cold start, in this order: [ARCHITECTURE.md](ARCHITECTURE.md), this file, `PLAN.md`, `IMPLEMENTATION_CONTRACT.md`. `ARCHITECTURE.md` is the agreed proposal and the architecture. The other three do not restate it. A session that has not read `ARCHITECTURE.md` cannot continue.

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` becomes the clean-checkout command in the production-note MR. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/data-quality`, from `main` at `b7fc06a`. Analytic outputs and earlier waves are merged.

Current wave: data quality, ready for user review. The accepted stage history guard rejects overlapping SCD2 intervals before Iceberg writes. The curated DQ gate checks population and comparable GDP per capita, records scoped disappearances without changing retained facts, sends a local SNS warning when the count jumps, and blocks export on a hard failure. Both code reviews passed and the live fixtures closed the remaining validation. The writer fixture proved warning acknowledgement, recovery when omitted data returns, and same-ID retry after a hard quality failure.

Validation: all eight `make test` selector suites passed across local serial runs (43 offline tests and 11 live tests). Compose configuration, selector dry-runs and `git diff --check` passed. Two overlapping private stacks exhausted the Colima VM's memory and timed out. Both stacks were removed and the affected suites passed serially.

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
| data quality | feature | complete locally, awaiting user review |
| production note | feature | pending |

Open decisions: none. See `QUESTIONS_AND_DECISIONS.md`.

Implementation blockers: none identified.
