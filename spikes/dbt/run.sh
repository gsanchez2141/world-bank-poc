#!/bin/sh
set -eu

cd /usr/app

run_step() {
  name=$1
  shift
  if "$@"; then
    echo "${name}: pass"
  else
    echo "${name}: fail" >&2
    exit 1
  fi
}

run_step "dbt run" dbt run --project-dir /usr/app --profiles-dir /usr/app
run_step "dbt test" dbt test --project-dir /usr/app --profiles-dir /usr/app
run_step "dbt compile" dbt compile --static-analysis baseline --project-dir /usr/app --profiles-dir /usr/app
run_step "dbt docs generate" dbt docs generate --project-dir /usr/app --profiles-dir /usr/app

if [ ! -f /usr/app/target/manifest.json ] || [ ! -f /usr/app/target/index.html ]; then
  echo "dbt artifacts: fail" >&2
  exit 1
fi
echo "dbt artifacts: pass"
