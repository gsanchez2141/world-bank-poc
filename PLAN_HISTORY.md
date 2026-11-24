# Plan history

Closed waves. The live plan is `PLAN.md`.

## Platform

Closed. Merged to `main` as `9cf28c8` (pull request #1).

Infra compose, Terraform for the three buckets, SNS, Secrets Manager, and CloudWatch Logs, the `dbt-postgres` image, Iceberg and dbt spikes, `make platform`, `make test`, `make test-spike`, and `make down`.

`make test` in that MR prints `test suites are not in this MR` and exits 0. `make test-spike` runs the spikes. `make demo` and `make run` exit non-zero because the pipeline DAG was not in that MR.

The email ciphertext was created by the user during this MR. Apply succeeds when that file or its key is absent.

User reviews each MR before the next one starts. That review happened: `make platform`, `make test-spike`, and `make down` worked, and the user merged.

## Ingest

Merged to `main` as `af1b325` (pull request #2).

Ingest. Image `lakehouse-ingest-world-bank`, raw responses, record batches of at most 100 rows, attempt prefixes, the raw manifest, CloudWatch logs for that image, and the ingest tests. `make test` runs those tests. This MR had no DAG.
