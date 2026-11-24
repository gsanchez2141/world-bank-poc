# Project status history

Closed waves. The live status is `PROJECT_STATUS.md`.

## Platform

Branch `feat/local-platform`, merged to `main` as `9cf28c8`.

`make platform` with no email file exits 0. `make test` exits 0. `make test-spike` covers the Iceberg spike, including a recreated emulator whose Postgres volume still pointed at a missing Iceberg metadata object, and the dbt spike. `make down` keeps the Postgres volume. `make demo` exits non-zero and prints `pipeline DAG is not in this MR`.

DuckDB 2.0.1 commits an SCD2 close and insert as two snapshots, overwrite then append. dbt 2.0.12 passed `run`, `test`, `compile --static-analysis baseline`, and `docs generate`.

`scripts/emulator-env.sh` prepends `~/.local/bin` so Terraform resolves for that process only. That fix is in the merge.

Auditor scopes for infra, email, Iceberg, dbt, and the test-target rename were merge-ready.

## Ingest

Branch `feat/ingest`.

Current wave: standalone World Bank raw ingestion. Implementation and review are complete. The image fetches or replays raw responses, publishes traceable batches and manifests, and reports run outcomes. The [ingest contract](IMPLEMENTATION_CONTRACT_HISTORY.md#wave-ingest--standalone-world-bank-raw-ingestion) was accepted, and the Auditor passed the focused rereview after correction attempt 1 restored independent SNS notification when CloudWatch stream creation fails. The user merged pull request #2 to `main` as `af1b325`.

Validation: `make test` built the image and passed six offline unit tests and the local Postgres/AWS emulator integration scenario covering failure, retry, repeat, replay, log retrieval, and SNS acknowledgement. Compose config, `make -n test-spike`, and whitespace checks passed. The tests used saved World Bank responses. Unit-only coverage was 53% and is report-only. The README documents operation and replaces the platform-only test instructions. No contract or decision changes were needed after implementation.

## Stage

Merged PR #3 into main as `e921316`.

Current wave: standalone Iceberg SCD2 and snapshot recovery. The [stage contract](IMPLEMENTATION_CONTRACT_HISTORY.md#wave-stage--standalone-iceberg-scd2-and-snapshot-recovery) passed the acceptance gate. The stage job validates completed raw inputs, commits changed versions, and records the original snapshot IDs even when retry follows a later Iceberg commit. The Auditor passed focused rereview after correction attempt 1 made every expected raw request mandatory. The user merged the reviewed stage implementation.

Validation: `make test` built both job images and passed six ingest and five stage offline unit tests, plus both local integration scenarios. Stage verified Iceberg day partitioning, unchanged runs with no files or snapshots added, older replay and resume, omitted keys, null transitions, exact recovery after commit, and raw-input rejection with CloudWatch and SNS. Compose configuration, `make -n test-spike`, and whitespace checks passed. Stage unit-only coverage was 33% and is report-only. The README documents stage invocation and the local suite. No architecture or decision changes were needed.

## Consumer

Merged PR #4 into main as `705efe0`.

Current wave: Postgres consumer. Load exact historical Iceberg snapshots into `stg`, retain provenance, and make a same-ID retry atomic. Contract Acceptance Gate: **Accepted for Machinist**. See [Exact Iceberg commit load into Postgres](IMPLEMENTATION_CONTRACT_HISTORY.md#wave-consumer--exact-iceberg-commit-load-into-postgres).

Validation: `make test` passed 15 offline unit tests and all three local integration scenarios. The consumer integration verified exact historical loads, lossless decimals, reprocess and zero-change completion, missing-snapshot rejection, and rollback of a failed replacement. Compose configuration, the spike target dry run, and whitespace checks passed. The Auditor passed the consumer scope on the first review.

The user approved a narrow follow-up to skip completed same-ID consumer retries. The focused implementation and Auditor review passed for [Return immediately for a completed load](IMPLEMENTATION_CONTRACT_HISTORY.md#task-consumer-completed-retry--return-immediately-for-a-completed-load). The packaged integration verified that an already-complete retry succeeds with an unavailable snapshot and staging writes guarded. An unfinished retry and distinct reprocess remain covered.

## Curated

Merged PR #5 into main as `54e3542`.

Current wave: curated tables. Build the dbt star from consumer staging, retain provenance, and publish catalog artifacts. The user selected dbt Core 1.12.7 with native model-level lineage and catalog. Contract Acceptance Gate: **Accepted for Machinist** for [Core Postgres star and native dbt catalog](IMPLEMENTATION_CONTRACT_HISTORY.md#wave-curated--core-postgres-star-and-native-dbt-catalog). The independent test suite selection task was accepted, implemented and passed Auditor review. All three named installed suites and the aggregate passed against the local stack. The curated selector now runs the real curated unit and integration suites.

Validation: `make test curated`, the full `make test` (21 unit tests and four local integrations), and both platform spikes passed. The Auditor passed the curated scope with no blockers. The user merged curated before DAG wiring.

## Writer pipeline

Merged PR #6 into main as `c557a2f`.

Current wave: the `world_bank_pipeline` Airflow DAG. Orchestrate the reviewed job images with one writer, run gating and resume, visible failure, and `make run`. Contract Acceptance Gate: **Accepted for Machinist** for [Only writer DAG and Airflow run command](IMPLEMENTATION_CONTRACT_HISTORY.md#wave-world_bank_pipeline--only-writer-dag-and-airflow-run-command). Local DockerOperator launch and authenticated REST status probes passed. The Auditor passed focused rereview after correction attempt 1 made the trigger report its IDs before submission and made the gate reject non-object input before opening Postgres.

Validation: the full `make test` passed, including the disposable real Airflow/Docker fixture. After the trigger and gate corrections, `make test pipeline` passed seven offline tests and the same real fixture. It checks one active writer, exact DagRun status, gate-only XCom, failed ingestion and one SNS acknowledgement, abandonment, and same-ID resume without rerunning completed jobs. Compose configuration, selector dry runs, and whitespace checks passed.
