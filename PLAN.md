# Plan

Cold start reads [ARCHITECTURE.md](ARCHITECTURE.md) before this file. That document is the agreed proposal and the architecture. This file only says which merge request is current. It does not restate the design. A session that has not read `ARCHITECTURE.md` cannot continue.

One command from a clean checkout is `make demo`. No real AWS account, no paid resources, no push unless the user asks.

The platform, ingest, stage, and consumer are on `main`. Merge requests stay separate. The user reviews each MR before the next one starts.

## Now

Curated tables. Build the dbt star in Postgres from completed consumer loads. Keep stable surrogate keys, preserve source provenance and explicit nulls, and leave omitted observations in place. The reviewed job publishes native dbt model lineage and catalog locally. Curated awaits user review before pipeline DAG work.

## Next

`world_bank_pipeline`, `world_bank_backfill`, analytic outputs, data quality, and the production note.

## User checkpoints

Stop when an MR is ready, when a question needs a decision, or when the work leaves [ARCHITECTURE.md](ARCHITECTURE.md).
