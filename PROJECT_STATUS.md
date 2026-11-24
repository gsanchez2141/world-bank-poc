# Project status

Cold start, in this order: [ARCHITECTURE.md](ARCHITECTURE.md), this file, `PLAN.md`, `IMPLEMENTATION_CONTRACT.md`. `ARCHITECTURE.md` is the agreed proposal and the architecture. The other three do not restate it. A session that has not read `ARCHITECTURE.md` cannot continue.

Objective: local World Bank pipeline at `world-bank-poc`. `make demo` becomes the clean-checkout command in the production-note MR. All cloud calls stay on the AWS emulator at `https://127.0.0.1:9297`.

Branch: `feat/ingest`, from `main` at `9cf28c8`. Platform is merged.

Current wave: standalone World Bank raw ingestion. Implementation and review are complete. The image fetches or replays raw responses, publishes traceable batches and manifests, and reports run outcomes. The [ingest contract](IMPLEMENTATION_CONTRACT.md#wave-ingest--standalone-world-bank-raw-ingestion) was accepted, and the Auditor passed the focused rereview after correction attempt 1 restored independent SNS notification when CloudWatch stream creation fails. The reviewed changes are on local `feat/ingest`, awaiting user review before stage starts.

Validation: `make test` built the image and passed six offline unit tests and the local Postgres/AWS emulator integration scenario covering failure, retry, repeat, replay, log retrieval, and SNS acknowledgement. Compose config, `make -n test-spike`, and whitespace checks passed. The tests used saved World Bank responses. Unit-only coverage was 53% and is report-only. The README documents operation and replaces the platform-only test instructions. No contract or decision changes were needed after implementation.

Wave list:

| Wave | Type | Status |
|---|---|---|
| platform | feature | done, merged to main |
| ingest | feature | implemented and audited, awaiting user review |
| stage | feature | pending |
| consumer | feature | pending |
| curated | feature | pending |
| world_bank_pipeline | feature | pending |
| world_bank_backfill | feature | pending |
| analytic outputs | feature | pending |
| data quality | feature | pending |
| production note | feature | pending |

Open decisions: none. See `QUESTIONS_AND_DECISIONS.md`.

Implementation blockers: none. User review is the checkpoint before stage.
