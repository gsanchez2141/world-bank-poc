# Plan

Local World Bank lakehouse. One command from a clean checkout is `make demo`. No real AWS account, no paid resources, no push unless the user asks.

The approved design is the platform, ingest, stage, consumer, curated star, one writer DAG, a trigger-only backfill DAG, four views, queryable data-quality results, and a production note. Merge requests stay separate. The user reviews each MR before the next one starts.

## Now

Platform only. Infra compose, Terraform for the three buckets, SNS, Secrets Manager, and CloudWatch Logs, the `dbt-postgres` image, Iceberg and dbt spikes, `make platform`, `make test`, and `make down`.

## Next

Ingest, then stage, consumer, curated tables, `world_bank_pipeline`, `world_bank_backfill`, analytic outputs, data quality, and the production note. Each is its own MR.

## User checkpoints

Stop when an MR is ready, when a question needs a decision, or when the work leaves the approved design. The email ciphertext is created during the platform MR, after the secret exists, and only by the user. Apply must succeed when that file or its key is absent.
