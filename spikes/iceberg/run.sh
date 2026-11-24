#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
cd "$root"

if ! curl -sf http://127.0.0.1:8181/v1/config >/dev/null; then
  echo "error: iceberg catalog is down; run make platform" >&2
  exit 1
fi

container="$root/.spike-container.sh"
load_body=$(mktemp)
trap 'rm -f "$container" "$load_body"' EXIT
cat > "$container" << 'EOS'
#!/bin/sh
set -eu

mode=${1:?mode}
cred=/opt/emulator/emulator_credentials
ca=/opt/emulator/ca.pem

if [ ! -f "$cred" ] || [ ! -f "$ca" ]; then
  echo "error: emulator certificate or credentials are not mounted; run make platform" >&2
  exit 1
fi

EMULATOR_ACCESS_KEY_ID=$(awk -F= '/aws_access_key_id/ { gsub(/[[:space:]]/, "", $2); print $2; exit }' "$cred")
EMULATOR_SECRET_ACCESS_KEY=$(awk -F= '/aws_secret_access_key/ { gsub(/[[:space:]]/, "", $2); print $2; exit }' "$cred")
export EMULATOR_ACCESS_KEY_ID EMULATOR_SECRET_ACCESS_KEY

if [ -z "$EMULATOR_ACCESS_KEY_ID" ] || [ -z "$EMULATOR_SECRET_ACCESS_KEY" ]; then
  echo "error: emulator credentials file has no keys" >&2
  exit 1
fi

phase() {
  awk -v p="$1" '
    $0 == "-- PHASE " p { on = 1; next }
    substr($0, 1, 9) == "-- PHASE " { on = 0 }
    on { print }
  ' /opt/spike/scd2.sql
}

run_sql() {
  {
    phase attach
    printf '%s\n' "$1"
  } | duckdb :memory:
}

marker() {
  run_sql "$1" | tr -d '│|' | grep -o -E 'needs-change=[01]|current-row: (pass|fail)|read-current: (pass|fail)' | head -n 1
}

dump_snaps() {
  dest=$1
  python3 - "$dest" << 'PY'
import json, sys, urllib.request
dest = sys.argv[1]
url = "http://127.0.0.1:8181/v1/namespaces/spike/tables/scd2_probe"
with urllib.request.urlopen(url) as resp:
    doc = json.load(resp)
meta = doc.get("metadata") or {}
snaps = meta.get("snapshots") or []
current = meta.get("current-snapshot-id")
if current is None:
    sys.stderr.write("error: iceberg table has no current snapshot\n")
    sys.exit(1)
lines = ["current %s" % current]
for snap in snaps:
    summary = snap.get("summary") or {}
    op = summary.get("operation") or snap.get("operation") or "unknown"
    lines.append("%s %s" % (snap.get("snapshot-id"), op))
with open(dest, "w") as handle:
    handle.write("\n".join(lines) + "\n")
PY
}

if [ "$mode" = "read" ]; then
  assertion=$(marker "$(phase read)")
  echo "$assertion"
  case "$assertion" in
    *pass) exit 0 ;;
    *) exit 1 ;;
  esac
fi

run_sql "$(phase setup)" >/dev/null
dump_snaps /tmp/spike-before

needs=$(marker "$(phase needs-change)")
if [ "$needs" != "needs-change=1" ]; then
  echo "close-and-insert: fail" >&2
  exit 1
fi

run_sql "$(phase change)" >/dev/null
dump_snaps /tmp/spike-after

if ! python3 - << 'PY'
import sys
before = open("/tmp/spike-before").read().splitlines()[1:]
after = open("/tmp/spike-after").read().splitlines()[1:]
if after[:len(before)] != before:
    sys.exit(1)
new = after[len(before):]
if len(new) != 2:
    sys.exit(1)
overwrite_id, overwrite_op = new[0].split()
append_id, append_op = new[1].split()
if overwrite_op != "overwrite" or append_op != "append":
    sys.exit(1)
sys.stdout.write("overwrite_snapshot_id=%s\nappend_snapshot_id=%s\n" % (overwrite_id, append_id))
PY
then
  echo "close-and-insert: fail" >&2
  exit 1
fi
echo "close-and-insert: pass"

needs=$(marker "$(phase needs-change)")
if [ "$needs" != "needs-change=0" ]; then
  echo "unchanged-rerun: fail" >&2
  exit 1
fi

dump_snaps /tmp/spike-rerun
if ! cmp -s /tmp/spike-after /tmp/spike-rerun; then
  echo "unchanged-rerun: fail" >&2
  exit 1
fi
echo "unchanged-rerun: pass"

set +e
run_sql "$(phase fail)" >/tmp/spike-fail.out 2>/tmp/spike-fail.err
fail_status=$?
set -e
if [ "$fail_status" -eq 0 ] || ! grep -q 'forced-statement-failure' /tmp/spike-fail.err /tmp/spike-fail.out; then
  echo "rollback: fail" >&2
  echo "fail-status:$fail_status" >&2
  echo "fail-stderr:" >&2
  cat /tmp/spike-fail.err >&2
  echo "fail-stdout:" >&2
  cat /tmp/spike-fail.out >&2
  exit 1
fi

dump_snaps /tmp/spike-rollback
if ! cmp -s /tmp/spike-after /tmp/spike-rollback; then
  echo "rollback: fail" >&2
  echo "snapshots-after:" >&2
  cat /tmp/spike-after >&2
  echo "snapshots-rollback:" >&2
  cat /tmp/spike-rollback >&2
  exit 1
fi

assertion=$(marker "$(phase current-ok)")
case "$assertion" in
  *pass) echo "rollback: pass" ;;
  *) echo "rollback: fail" >&2; echo "current-row-marker:${assertion}" >&2; exit 1 ;;
esac
EOS

compose() {
  docker compose -f compose.infra.yaml -f compose.jobs.yaml "$@"
}

catalog_psql() {
  docker compose -f compose.infra.yaml exec -T postgres \
    psql -U postgres -d iceberg_catalog -v ON_ERROR_STOP=1 -tAc "$1"
}

# A recreated emulator drops the metadata object and keeps this catalog row.
# Dropping the table in DuckDB would load that stale row, so remove it first.
repair_stale_catalog() {
  row=$(catalog_psql "SELECT count(*) FROM iceberg_tables WHERE table_namespace = 'spike' AND table_name = 'scd2_probe';")
  row=$(printf '%s' "$row" | tr -d '[:space:]')
  if [ "$row" != "1" ]; then
    return 0
  fi

  code=$(curl -sS -o "$load_body" -w '%{http_code}' \
    http://127.0.0.1:8181/v1/namespaces/spike/tables/scd2_probe) || {
    echo "stale-catalog: fail" >&2
    exit 1
  }
  if [ "$code" = "200" ]; then
    return 0
  fi
  if ! grep -q 'NoSuchKeyException' "$load_body"; then
    echo "stale-catalog: fail" >&2
    exit 1
  fi

  deleted=$(catalog_psql "WITH removed AS (DELETE FROM iceberg_tables WHERE table_namespace = 'spike' AND table_name = 'scd2_probe' RETURNING 1) SELECT count(*) FROM removed;")
  deleted=$(printf '%s' "$deleted" | tr -d '[:space:]')
  if [ "$deleted" != "1" ]; then
    echo "stale-catalog: fail" >&2
    exit 1
  fi
  echo "stale-catalog: pass"
}

repair_stale_catalog

compose run --rm -T \
  -v "$container:/opt/spike/container.sh:ro" \
  --entrypoint /bin/sh \
  iceberg-spike /opt/spike/container.sh write
compose run --rm -T \
  -v "$container:/opt/spike/container.sh:ro" \
  --entrypoint /bin/sh \
  iceberg-spike /opt/spike/container.sh read
