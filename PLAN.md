# Plan

Cold start reads [ARCHITECTURE.md](ARCHITECTURE.md) before this file. That document is the agreed proposal and the architecture. This file only says which merge request is current. It does not restate the design. A session that has not read `ARCHITECTURE.md` cannot continue.

One command from a clean checkout is `make demo`. No real AWS account, no paid resources, no push unless the user asks.

The platform, job images, and writer DAG are on `main`. Merge requests stay separate. The user reviews each MR before the next one starts.

## Now

Analytic outputs. Publish four reporting views with calendar-year and missingness semantics, then produce local CSVs and emulator CSV and Parquet artifacts. The real writer fixture verified that a failed export retries from completed curated state. Analytic outputs are implemented and audited, awaiting user review before data quality.

## Next

Data quality, and the production note.

## User checkpoints

Stop when an MR is ready, when a question needs a decision, or when the work leaves [ARCHITECTURE.md](ARCHITECTURE.md).
