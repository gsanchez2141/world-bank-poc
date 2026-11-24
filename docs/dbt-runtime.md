# Local dbt runtime

The local star uses dbt Core 1.12.7 and dbt-postgres 1.11.0. The native manifest records model and source dependencies. Column-level lineage is outside the approved scope.

The prior Fusion 2.0.12/Postgres adapter passed the simple platform spike and a scoped incremental probe. A two-model strict-lineage probe panicked during `PostgreSQL's list_relations_schemas`. A separate `--write-catalog` probe wrote `catalog.json`, but its metadata SQL contained a bare `None` and introspection failed. An earlier plain docs path omitted `catalog.json`. The user approved the Core runtime and model-level lineage scope after these probes.

A local Core probe built and tested two Postgres models, then generated a native catalog with both relations. Run the project and inspect the native artifacts with:

```sh
dbt build --project-dir /usr/app/curated --profiles-dir /usr/app/curated --target-path /tmp/world-bank-target
dbt docs generate --project-dir /usr/app/curated --profiles-dir /usr/app/curated --target-path /tmp/world-bank-target
```

Run these inside the `dbt-postgres` image with the local Postgres service reachable. The one-shot curated job creates a fresh target and publishes the validated native JSON files.
