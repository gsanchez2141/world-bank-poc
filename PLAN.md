# Plan

Cold start reads [ARCHITECTURE.md](ARCHITECTURE.md) before this file. That document is the agreed proposal and the architecture. This file only says which merge request is current. It does not restate the design. A session that has not read `ARCHITECTURE.md` cannot continue.

One command from a clean checkout is `make demo`. No real AWS account, no paid resources, no push unless the user asks.

The platform and ingest are on `main`. Merge requests stay separate. The user reviews each MR before the next one starts.

## Now

Stage. The `lakehouse-stage-world-bank` image reads completed raw extracts and replay selections, validates lineage, and records changed versions in the four Iceberg tables. It publishes the snapshot IDs and changed count needed by the consumer. A retry reconciles a committed snapshot without writing another version.

Acceptance on saved responses covers initial stage, an unchanged rerun, historical corrections, numeric-to-null and null-to-numeric transitions, omitted keys, older replays and resumes, malformed raw inputs, and recovery after Iceberg commits before Postgres records the result. The stage test suite exercises the packaged job against the local emulator, REST catalog, and warehouse. Stage is implemented and audited locally. It awaits user review before consumer begins. Curated outputs and DAG wiring are later merge requests.

## Next

Consumer, curated tables, `world_bank_pipeline`, `world_bank_backfill`, analytic outputs, data quality, and the production note.

## User checkpoints

Stop when an MR is ready, when a question needs a decision, or when the work leaves [ARCHITECTURE.md](ARCHITECTURE.md).
