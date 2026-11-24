"""Read and validate the exact Iceberg changes selected by a stage run."""
from datetime import datetime, timezone
from decimal import Decimal
import re

from world_bank_stage.iceberg import fields, literal
from world_bank_stage.scd2 import KEY_COLUMNS

TABLES = ("observation", "country", "indicator", "income_level")


def _time(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00").replace(" ", "T"))
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError("invalid stage timestamp")
    return value


def _snapshot_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*", value):
        raise ValueError("invalid snapshot ID")
    result = int(value)
    if result > 2**63 - 1:
        raise ValueError("snapshot ID out of range")
    return result


def validate_manifest(source_row):
    if not source_row or source_row.get("stage_status") != "complete" or source_row.get("mode") not in ("extract", "replay"):
        raise ValueError("source stage commit is not complete")
    _time(source_row.get("stage_ingested_at"))
    entries = source_row.get("stage_snapshot_ids")
    if not isinstance(entries, dict) or set(entries) != set(TABLES):
        raise ValueError("invalid stage manifest")
    total = 0
    for table, entry in entries.items():
        if not isinstance(entry, dict) or set(entry) != {"state", "overwrite", "append", "changed_count"} or entry["state"] != "complete":
            raise ValueError("invalid stage table manifest")
        count = entry["changed_count"]
        if type(count) is not int or count < 0:
            raise ValueError("invalid stage count")
        if count == 0:
            if entry["append"] is not None or entry["overwrite"] is not None:
                raise ValueError("unexpected snapshot for unchanged table")
        else:
            _snapshot_id(entry["append"])
            if entry["overwrite"] is not None:
                _snapshot_id(entry["overwrite"])
        total += count
    if type(source_row.get("stage_changed_count")) is not int or total != source_row["stage_changed_count"]:
        raise ValueError("stage total mismatch")
    return entries


def _required(table, row):
    optional = {"valid_to"}
    optional.update({"value"} if table == "observation" else ())
    optional.update({"country_name", "income_level_id"} if table == "country" else ())
    optional.update({"indicator_name"} if table == "indicator" else ())
    optional.update({"income_level_name"} if table == "income_level" else ())
    for name, _ in fields(table):
        if name not in row or (name not in optional and row[name] is None):
            raise ValueError("missing required stage field")
    for name in ("raw_response_key", "response_sha256", "raw_object_key", "source_url", "ingestion_id", "stage_run_id", "record_hash", "api_payload"):
        if not isinstance(row[name], str) or not row[name]:
            raise ValueError("invalid stage provenance")
    if table == "observation" and row["value"] is not None:
        row["value"] = Decimal(row["value"])
    for name in ("raw_ingested_at", "stage_ingested_at", "valid_from"):
        row[name] = _time(row[name])
    if row["valid_to"] is not None:
        row["valid_to"] = _time(row["valid_to"])


def _read(catalog, table, info, snapshot):
    location = info["metadata-location"]
    expected_prefix = catalog.location(table) + "/metadata/"
    if not location.startswith(expected_prefix) or ".." in location[len(expected_prefix):].split("/"):
        raise ValueError("invalid metadata location")
    projection = ", ".join("CAST(value AS VARCHAR) AS value" if name == "value" and table == "observation" else name for name, _ in fields(table))
    return catalog.query(f"SELECT {projection} FROM iceberg_scan({literal(location)}, snapshot_from_id = {snapshot});")


def read_changes(source_row, catalog_factory):
    manifest = validate_manifest(source_row)
    output = {table: [] for table in TABLES}
    if source_row["stage_changed_count"] == 0:
        return output
    catalog = catalog_factory()
    stage_time = _time(source_row["stage_ingested_at"])
    for table, entry in manifest.items():
        if not entry["changed_count"]:
            continue
        info = catalog.inspect(table)
        if not info:
            raise ValueError("stage table missing")
        snapshots = {str(item["snapshot-id"]): item for item in info["metadata"].get("snapshots", [])}
        append_id = entry["append"]
        overwrite_id = entry["overwrite"]
        append = snapshots.get(append_id)
        if not append or append.get("summary", {}).get("operation") != "append":
            raise ValueError("missing append snapshot")
        if overwrite_id:
            overwrite = snapshots.get(overwrite_id)
            if not overwrite or overwrite.get("summary", {}).get("operation") != "overwrite" or str(append.get("parent-snapshot-id")) != overwrite_id:
                raise ValueError("invalid overwrite snapshot")
        rows = _read(catalog, table, info, _snapshot_id(append_id))
        opened = [row for row in rows if row.get("stage_run_id") == source_row["ingestion_id"] and _time(row.get("stage_ingested_at")) == stage_time and _time(row.get("valid_from")) == stage_time and row.get("is_current") is True and row.get("valid_to") is None]
        keys = KEY_COLUMNS[table]
        if len(opened) != entry["changed_count"] or len({tuple(row[k] for k in keys) for row in opened}) != len(opened):
            raise ValueError("unexpected append rows")
        closed = []
        if overwrite_id:
            rows = _read(catalog, table, info, _snapshot_id(overwrite_id))
            closed = [row for row in rows if row.get("valid_to") is not None and _time(row["valid_to"]) == stage_time and row.get("is_current") is False]
        by_key = {}
        for row in closed:
            _required(table, row)
            key = tuple(row[k] for k in keys)
            if key in by_key:
                raise ValueError("duplicate closed row")
            by_key[key] = row
        for row in opened:
            _required(table, row)
            key = tuple(row[k] for k in keys)
            previous = by_key.pop(key, None)
            if row["version"] == 1:
                if previous is not None:
                    raise ValueError("unexpected close")
            elif previous is None or previous["version"] != row["version"] - 1 or previous["valid_from"] >= stage_time:
                raise ValueError("missing matching close")
            row.update(change_kind="append", snapshot_id=_snapshot_id(append_id))
        if by_key:
            raise ValueError("unmatched close")
        for row in closed:
            row.update(change_kind="close", snapshot_id=_snapshot_id(overwrite_id))
        output[table] = closed + opened
    return output
