# Plan

Cold start reads [ARCHITECTURE.md](ARCHITECTURE.md) before this file. That document is the agreed proposal and the architecture. This file only says which merge request is current. It does not restate the design. A session that has not read `ARCHITECTURE.md` cannot continue.

One command from a clean checkout is `make demo`. No real AWS account, no paid resources, no push unless the user asks.

The platform, ingest, and stage are on `main`. Merge requests stay separate. The user reviews each MR before the next one starts.

## Now

Consumer. Load the exact completed stage snapshots into Postgres `stg`, preserving provenance. A retry replaces rows for that ingestion ID atomically. An unchanged stage run completes without inserting rows. The local suite verified recovery after Iceberg commits and atomic warehouse rollback. Consumer is implemented and audited, awaiting user review before curated work.

## Next

Curated tables, `world_bank_pipeline`, `world_bank_backfill`, analytic outputs, data quality, and the production note.

## User checkpoints

Stop when an MR is ready, when a question needs a decision, or when the work leaves [ARCHITECTURE.md](ARCHITECTURE.md).
