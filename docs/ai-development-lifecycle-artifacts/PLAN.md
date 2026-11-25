# Plan

Cold start reads [ARCHITECTURE.md](../../ARCHITECTURE.md) before this file. That document is the agreed proposal and the architecture. This file only says which merge request is current. It does not restate the design. A session that has not read `ARCHITECTURE.md` cannot continue.

One command from a clean checkout is `make demo`. No real AWS account, no paid resources, no push unless the user asks.

The platform, job images, and writer DAG are on `main`. Merge requests stay separate. The user reviews each MR before the next one starts.

## Now

The production note and local demo are complete. The final changes are grouped in local commits on `feat/production-demo` and ready for user review. See [PLAN_HISTORY.md](PLAN_HISTORY.md#production-design-and-local-demo) for the completed scope.

## User checkpoints

Stop when an MR is ready, when a question needs a decision, or when the work leaves [ARCHITECTURE.md](../../ARCHITECTURE.md).
