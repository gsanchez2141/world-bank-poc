# Plan

Cold start reads [ARCHITECTURE.md](ARCHITECTURE.md) before this file. That document is the agreed proposal and the architecture. This file only says which merge request is current. It does not restate the design. A session that has not read `ARCHITECTURE.md` cannot continue.

One command from a clean checkout is `make demo`. No real AWS account, no paid resources, no push unless the user asks.

The platform, job images, and writer DAG are on `main`. Merge requests stay separate. The user reviews each MR before the next one starts.

## Now

`world_bank_backfill`. Trigger the single writer DAG with a validated scope, then wait for its exact outcome. Cover extract, replay, reprocess, and rebuilding the current Iceberg state into an empty warehouse. The local fixture verified that scoped backfill preserves data outside its scope. Backfill is implemented and audited, awaiting user review before analytic outputs.

## Next

Analytic outputs, data quality, and the production note.

## User checkpoints

Stop when an MR is ready, when a question needs a decision, or when the work leaves [ARCHITECTURE.md](ARCHITECTURE.md).
