# Questions and decisions

The agreed proposal and architecture is [ARCHITECTURE.md](ARCHITECTURE.md). Read that file before this one. The decisions below are constraints inside it. This file does not restate the design.

No open questions.

## Decided

- Warehouse is Postgres. dbt 2.0.12 is tried first. dbt Core 1.x is the fallback if the Postgres adapter fails the platform spike.
- Images: `lakehouse-ingest-world-bank`, `lakehouse-stage-world-bank`, `lakehouse-consumer-postgres`, `dbt-postgres`.
- One writer DAG, `world_bank_pipeline`, `max_active_runs=1`. `world_bank_backfill` only triggers it.
- No `meta.pipeline_lock`. No `meta.observation_coverage`.
- A key omitted from a later in-scope extract stays on the fact. The warning is a row in `meta.dq_result`. The missing view reads that row.
- Email is optional. Ciphertext may be committed. The key is not. Apply skips the subscription when the key is absent and fails only that step when the key is present and invalid.
- Logs go to CloudWatch Logs on the emulator.
- The user reviews every MR before the next MR starts.
