# world-bank-poc

Local platform for the World Bank lakehouse proof of concept. AWS calls stay on the emulator at `https://127.0.0.1:9297`. This checkout does not call AWS or the World Bank API.

## Local platform

From this merge request:

1. `make platform` starts the emulator, Postgres, the Iceberg REST catalog, and Airflow with an empty DAG folder. It builds the `dbt-postgres` image and applies the dev Terraform stack to the emulator.
2. `make test` prints `test suites are not in this MR` and exits 0. It does not run a test suite, and it does not call the emulator, dbt, or DuckDB.
3. `make test-spike` runs the Iceberg SCD2 spike and the dbt spike. If the emulator or the warehouse is down, run `make platform` first.
4. `make down` stops the stack and keeps the Postgres volume.

`make demo` is not the clean-checkout command in this MR. `make demo` and `make run` exit with an error because the pipeline DAG is not in this MR.
