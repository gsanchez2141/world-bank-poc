# Plan

Cold start reads [ARCHITECTURE.md](ARCHITECTURE.md) before this file. That document is the agreed proposal and the architecture. This file only says which merge request is current. It does not restate the design. A session that has not read `ARCHITECTURE.md` cannot continue.

One command from a clean checkout is `make demo`. No real AWS account, no paid resources, no push unless the user asks.

The platform is on `main`. Merge requests stay separate. The user reviews each MR before the next one starts.

## Now

Ingest. Image `lakehouse-ingest-world-bank`, raw responses, record batches of at most 100 rows, attempt prefixes, the raw manifest, CloudWatch logs for that image, and the ingest tests. `make test` runs those tests. This MR has no DAG.

## Next

Stage, then consumer, curated tables, `world_bank_pipeline`, `world_bank_backfill`, analytic outputs, data quality, and the production note.

## User checkpoints

Stop when an MR is ready, when a question needs a decision, or when the work leaves [ARCHITECTURE.md](ARCHITECTURE.md).
