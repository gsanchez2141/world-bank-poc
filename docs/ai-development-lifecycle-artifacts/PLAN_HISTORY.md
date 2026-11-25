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

## Stage

Stage. The `lakehouse-stage-world-bank` image reads completed raw extracts and replay selections, validates lineage, and records changed versions in the four Iceberg tables. It publishes the snapshot IDs and changed count needed by the consumer. A retry reconciles a committed snapshot without writing another version.

Acceptance on saved responses covers initial stage, an unchanged rerun, historical corrections, numeric-to-null and null-to-numeric transitions, omitted keys, older replays and resumes, malformed raw inputs, and recovery after Iceberg commits before Postgres records the result. The stage test suite exercises the packaged job against the local emulator, REST catalog, and warehouse. Stage is implemented and audited locally. Merged in PR #3 as e921316. Curated outputs and DAG wiring are later merge requests.

## Consumer

Consumer. Load the exact completed stage snapshots into Postgres `stg`, preserving provenance. A retry replaces rows for that ingestion ID atomically. An unchanged stage run completes without inserting rows. The local suite verified recovery after Iceberg commits and atomic warehouse rollback. Merged in PR #4 as 705efe0.

## Curated

Curated tables. Build the dbt star in Postgres from completed consumer loads. Keep stable surrogate keys, preserve source provenance and explicit nulls, and leave omitted observations in place. The reviewed job publishes native dbt model lineage and catalog locally. Merged in PR #5 as 54e3542.

## Writer pipeline

`world_bank_pipeline`. A single-writer Airflow DAG orchestrates the existing jobs, gates and resumes incomplete runs through the warehouse manifest, records failures, and exposes `make run` with the real DAG outcome. The writer DAG is implemented and audited locally. Merged in PR #6 as c557a2f. Reporting exports and `make demo` remain later work.

## Backfill

`world_bank_backfill`. Trigger the single writer DAG with a validated scope, then wait for its exact outcome. Cover extract, replay, reprocess, and rebuilding the current Iceberg state into an empty warehouse. The local fixture verified that scoped backfill preserves data outside its scope. Backfill is implemented and audited, merged in PR #7 as 9999d71.

## Analytic outputs

Analytic outputs. Publish four reporting views with calendar-year and missingness semantics, then produce local CSVs and emulator CSV and Parquet artifacts. The real writer fixture verified that a failed export retries from completed curated state. Analytic outputs are implemented and audited, merged in PR #8 as b7fc06a.

## Data quality

Merged in PR #9. Stage rejects invalid SCD2 history. Curated quality checks gate export, persist scoped disappearance findings, and notify SNS on comparable warning jumps. The live fixture proved missingness clearance and same-ID resume.

## Production design and local demo

The production note and clean-checkout `make demo` are complete locally and ready for user review. The command starts the local platform, waits for the writer and a bounded Brazil 2021 population backfill, and prints the four CSV paths. A live run verified successful exports and optional email setup with no decryption key. The design note documents the AWS and Snowflake mapping, Iceberg maintenance and operating tradeoffs.

The final scope also includes the missing-data demo, local stack reset, scheduled DAG pause default, and sample reports.
