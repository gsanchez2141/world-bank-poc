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
