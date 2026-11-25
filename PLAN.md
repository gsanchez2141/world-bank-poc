# Plan

Cold start reads [ARCHITECTURE.md](ARCHITECTURE.md) before this file. That document is the agreed proposal and the architecture. This file only says which merge request is current. It does not restate the design. A session that has not read `ARCHITECTURE.md` cannot continue.

One command from a clean checkout is `make demo`. No real AWS account, no paid resources, no push unless the user asks.

The platform, job images, and writer DAG are on `main`. Merge requests stay separate. The user reviews each MR before the next one starts.

## Now

Data quality is complete locally and ready for user review. Stage rejects invalid SCD2 history. The writer checks curated numeric invariants, records in-scope disappearance without changing retained facts, and alerts on warning-count jumps. Local tests verified clearance after observations return and failure recovery before export.

## Next

Production note and full local demo.

## User checkpoints

Stop when an MR is ready, when a question needs a decision, or when the work leaves [ARCHITECTURE.md](ARCHITECTURE.md).
