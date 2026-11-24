# Project status

Cold start, in this order: [ARCHITECTURE.md](ARCHITECTURE.md), this file, `PLAN.md`, `IMPLEMENTATION_CONTRACT.md`. `ARCHITECTURE.md` is the agreed proposal and the architecture. The other three do not restate it. A session that has not read `ARCHITECTURE.md` cannot continue.

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` becomes the clean-checkout command in the production-note MR. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/world-bank-backfill`, from `main` at `c557a2f`. The job images and writer DAG are merged.

Current wave: `world_bank_backfill`. Trigger the writer and wait for its exact outcome. Enable rebuild from current Iceberg state into an empty warehouse. Contract Acceptance Gate: **Accepted for Machinist** for [Restore all current Iceberg rows into an empty warehouse](IMPLEMENTATION_CONTRACT.md#task-writer-rebuild--restore-all-current-iceberg-rows-into-an-empty-warehouse). The writer rebuild task passed the bounded Auditor review, consumer suite, and real private scheduler fixture. Contract Acceptance Gate: **Accepted for Machinist** for [Forward validated scope and wait for the exact writer run](IMPLEMENTATION_CONTRACT.md#task-backfill-trigger--forward-validated-scope-and-wait-for-the-exact-writer-run).

Validation: `make test consumer`, `make test pipeline`, `make test backfill`, and the full `make test` passed. The aggregate ran 35 unit tests and six local integrations. Writer rebuild and parent trigger passed their bounded reviews. The final Airflow fixture verified manual child classification, queueing, exact failed-child propagation, narrowed scope preservation and all four modes.

Wave list:

| Wave | Type | Status |
|---|---|---|
| platform | feature | done, merged to main |
| ingest | feature | done, merged to main |
| stage | feature | done, merged to main |
| consumer | feature | done, merged to main |
| curated | feature | done, merged to main |
| world_bank_pipeline | feature | done, merged to main |
| world_bank_backfill | feature | implemented and audited, awaiting user review |
| analytic outputs | feature | pending |
| data quality | feature | pending |
| production note | feature | pending |

Open decisions: none. See `QUESTIONS_AND_DECISIONS.md`.

Implementation blockers: none. User review is the checkpoint before analytic outputs.
