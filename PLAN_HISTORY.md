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
