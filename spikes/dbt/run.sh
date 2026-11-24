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
run_step "dbt compile" dbt compile --project-dir /usr/app --profiles-dir /usr/app
run_step "dbt docs generate" dbt docs generate --project-dir /usr/app --profiles-dir /usr/app

if [ ! -s /usr/app/target/manifest.json ] || [ ! -s /usr/app/target/catalog.json ]; then
  echo "dbt artifacts: fail" >&2
  exit 1
fi
python - <<'PYTEST'
import json
from pathlib import Path
manifest = json.loads(Path("/usr/app/target/manifest.json").read_text())
catalog = json.loads(Path("/usr/app/target/catalog.json").read_text())
if not manifest.get("nodes") or not catalog.get("nodes") or catalog.get("errors"):
    raise SystemExit("dbt artifacts: fail")
PYTEST
echo "dbt artifacts: pass"
