# Plan

Cold start reads [ARCHITECTURE.md](ARCHITECTURE.md) before this file. That document is the agreed proposal and the architecture. This file only says which merge request is current. It does not restate the design. A session that has not read `ARCHITECTURE.md` cannot continue.

One command from a clean checkout is `make demo`. No real AWS account, no paid resources, no push unless the user asks.

The platform, ingest, stage, consumer, and curated jobs are on `main`. Merge requests stay separate. The user reviews each MR before the next one starts.

## Now

`world_bank_pipeline`. A single-writer Airflow DAG orchestrates the existing jobs, gates and resumes incomplete runs through the warehouse manifest, records failures, and exposes `make run` with the real DAG outcome. The writer DAG is implemented and audited locally. It awaits user review before backfill. Reporting exports and `make demo` remain later work.

## Next

`world_bank_backfill`, analytic outputs, data quality, and the production note.

## User checkpoints

Stop when an MR is ready, when a question needs a decision, or when the work leaves [ARCHITECTURE.md](ARCHITECTURE.md).
