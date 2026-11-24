# Project status

Cold start, in this order: [ARCHITECTURE.md](ARCHITECTURE.md), this file, `PLAN.md`, `IMPLEMENTATION_CONTRACT.md`. `ARCHITECTURE.md` is the agreed proposal and the architecture. The other three do not restate it. A session that has not read `ARCHITECTURE.md` cannot continue.

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` becomes the clean-checkout command in the production-note MR. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/stage`, from `main` at `af1b325`. Platform and ingest are merged.

Current wave: standalone Iceberg SCD2 and snapshot recovery. The [stage contract](IMPLEMENTATION_CONTRACT.md#wave-stage--standalone-iceberg-scd2-and-snapshot-recovery) passed the acceptance gate. The stage job validates completed raw inputs, commits changed versions, and records the original snapshot IDs even when retry follows a later Iceberg commit. The Auditor passed focused rereview after correction attempt 1 made every expected raw request mandatory. The implementation and review are complete on local `feat/stage`, awaiting user review before consumer starts.

Validation: `make test` built both job images and passed six ingest and five stage offline unit tests, plus both local integration scenarios. Stage verified Iceberg day partitioning, unchanged runs with no files or snapshots added, older replay and resume, omitted keys, null transitions, exact recovery after commit, and raw-input rejection with CloudWatch and SNS. Compose configuration, `make -n test-spike`, and whitespace checks passed. Stage unit-only coverage was 33% and is report-only. The README documents stage invocation and the local suite. No architecture or decision changes were needed.

Wave list:

| Wave | Type | Status |
|---|---|---|
| platform | feature | done, merged to main |
| ingest | feature | done, merged to main |
| stage | feature | implemented and audited, awaiting user review |
| consumer | feature | pending |
| curated | feature | pending |
| world_bank_pipeline | feature | pending |
| world_bank_backfill | feature | pending |
| analytic outputs | feature | pending |
| data quality | feature | pending |
| production note | feature | pending |

Open decisions: none. See `QUESTIONS_AND_DECISIONS.md`.

Implementation blockers: none. User review is the checkpoint before consumer.
