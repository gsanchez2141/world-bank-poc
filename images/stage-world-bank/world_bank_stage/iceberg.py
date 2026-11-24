"""REST table definitions, DuckDB writes and snapshot recovery."""
import configparser
import json
import os
from pathlib import Path
import subprocess
from urllib.parse import quote

import requests

from .scd2 import KEY_COLUMNS, timestamp

BUSINESS = {
    "observation": [("country_iso3", "string"), ("indicator_code", "string"), ("year", "int"), ("value", "decimal(38, 18)")],
    "country": [("country_iso3", "string"), ("country_name", "string"), ("income_level_id", "string")],
    "indicator": [("indicator_code", "string"), ("indicator_name", "string")],
    "income_level": [("income_level_id", "string"), ("income_level_name", "string")],
}
COMMON = [
    ("api_payload", "string"), ("record_hash", "string"), ("raw_response_key", "string"),
    ("raw_record_index", "int"), ("response_sha256", "string"), ("raw_object_key", "string"),
    ("source_url", "string"), ("api_page", "int"), ("raw_ingested_at", "timestamptz"),
    ("ingestion_id", "string"), ("stage_run_id", "string"), ("stage_ingested_at", "timestamptz"),
    ("version", "int"), ("valid_from", "timestamptz"), ("valid_to", "timestamptz"), ("is_current", "boolean")
]
ATTACH = """
LOAD httpfs;
LOAD iceberg;
SET ca_cert_file='/opt/emulator/ca.pem';
SET enable_server_cert_verification=true;
SET enable_progress_bar=false;
CREATE OR REPLACE SECRET s3_emulator (
 TYPE s3, PROVIDER config, KEY_ID getenv('EMULATOR_ACCESS_KEY_ID'),
 SECRET getenv('EMULATOR_SECRET_ACCESS_KEY'), REGION 'us-east-1',
 ENDPOINT '127.0.0.1:9297', URL_STYLE 'path', USE_SSL true
);
ATTACH 'lakehouse-stage' AS ice (TYPE iceberg, URI 'http://127.0.0.1:8181', AUTHORIZATION_TYPE 'none');
"""


def literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def fields(table):
    return BUSINESS[table] + COMMON


class Catalog:
    def __init__(self, namespace):
        self.namespace = namespace
        self.session = requests.Session()
        self.session.trust_env = False
        self.root = "http://127.0.0.1:8181/v1"
        config = self.rest("GET", self.root + "/config", params={"warehouse": "lakehouse-stage"})
        prefix = config.get("overrides", {}).get("prefix") or config.get("defaults", {}).get("prefix")
        if prefix:
            self.root += "/" + quote(prefix, safe="")
        credentials = configparser.RawConfigParser()
        credentials.read("/opt/emulator/emulator_credentials")
        self.env = dict(os.environ, EMULATOR_ACCESS_KEY_ID=credentials["default"]["aws_access_key_id"],
                        EMULATOR_SECRET_ACCESS_KEY=credentials["default"]["aws_secret_access_key"])

    def rest(self, method, url, missing_ok=False, **kwargs):
        response = self.session.request(method, url, timeout=(5, 20), allow_redirects=False, **kwargs)
        if response.status_code == 404 and missing_ok:
            return None
        if not 200 <= response.status_code < 300:
            raise ValueError(f"catalog request failed with HTTP {response.status_code}")
        return response.json() if response.content else None

    def path(self, table):
        return f"{self.root}/namespaces/{self.namespace}/tables/{table}"

    def location(self, table):
        if self.namespace == "world_bank":
            return f"s3://lakehouse-stage/world-bank/{table}"
        return f"s3://lakehouse-stage/stage-tests/{self.namespace}/{table}"

    def inspect(self, table):
        result = self.rest("GET", self.path(table), missing_ok=True)
        if result is None:
            return None
        metadata = result["metadata"]
        schema = next(item for item in metadata["schemas"] if item["schema-id"] == metadata["current-schema-id"])
        actual = [(item["id"], item["name"], item["type"]) for item in schema["fields"]]
        expected = [(i, name, kind) for i, (name, kind) in enumerate(fields(table), 1)]
        expected = [(i, name, kind.replace(" ", "")) for i, name, kind in expected]
        actual = [(i, name, kind.replace(" ", "")) for i, name, kind in actual]
        part = next(item for item in metadata["partition-specs"] if item["spec-id"] == metadata["default-spec-id"])
        time_id = next(i for i, name, _ in expected if name == "stage_ingested_at")
        if (metadata["location"] != self.location(table) or actual != expected or
            metadata["format-version"] != 2 or len(part["fields"]) != 1 or
            part["fields"][0]["transform"] != "day" or part["fields"][0]["source-id"] != time_id):
            raise ValueError("existing stage table has incompatible metadata")
        return result

    def create(self, table):
        namespace_path = f"{self.root}/namespaces/{self.namespace}"
        if self.rest("GET", namespace_path, missing_ok=True) is None:
            self.rest("POST", self.root + "/namespaces", json={"namespace": [self.namespace]})
        columns = fields(table)
        schema = [dict(id=i, name=name, required=False, type=kind)
                  for i, (name, kind) in enumerate(columns, 1)]
        time_id = next(i for i, (name, _) in enumerate(columns, 1) if name == "stage_ingested_at")
        self.rest("POST", namespace_path + "/tables", json={
            "name": table, "location": self.location(table),
            "schema": {"type": "struct", "schema-id": 0, "fields": schema},
            "partition-spec": {"spec-id": 0, "fields": [
                {"name": "stage_ingested_at_day", "transform": "day", "source-id": time_id, "field-id": 1000}]},
            "properties": {"format-version": "2"},
        })
        return self.inspect(table)

    def query(self, sql, expect_rows=True):
        command = ".output /dev/null\n" + ATTACH + "\n"
        if expect_rows:
            command += ".output stdout\n"
        result = subprocess.run(["duckdb", "-json", "-bail"], input=command + sql,
                                capture_output=True, text=True, env=self.env)
        if result.returncode:
            raise ValueError("DuckDB execution failed")
        if not result.stdout.strip():
            return []
        return json.loads(result.stdout)

    def rows(self, table, snapshot=None, info=None):
        if info is None:
            info = self.inspect(table)
        if info is None:
            return []
        if snapshot is None:
            relation = f"ice.{self.namespace}.{table}"
        else:
            relation = f"iceberg_scan({literal(info['metadata-location'])}, snapshot_from_id = {int(snapshot)})"
        return self.query(f"SELECT * FROM {relation};")

    def write(self, table, closes, appends, candidates, run_id, stage_time):
        relation = f"ice.{self.namespace}.{table}"
        statements = ["BEGIN;"]
        close_conditions = []
        for close in closes:
            conditions = [f"{column}={literal(value)}" for column, value in zip(KEY_COLUMNS[table], close["key"])]
            conditions.extend((f"version={close['version']}", "is_current=true"))
            close_conditions.append("(" + " AND ".join(conditions) + ")")
        if close_conditions:
            statements.append(f"UPDATE {relation} SET is_current=false,valid_to={literal(stage_time)} WHERE " + " OR ".join(close_conditions) + ";")
        insert_values = []
        for append in appends:
            candidate = candidates[tuple(append["key"])]
            row = dict(candidate, stage_run_id=run_id, stage_ingested_at=stage_time,
                       version=append["version"], valid_from=stage_time, valid_to=None, is_current=True)
            values = ", ".join(literal(row.get(name)) for name, _ in fields(table))
            insert_values.append(f"({values})")
        if insert_values:
            statements.append(f"INSERT INTO {relation} VALUES " + ", ".join(insert_values) + ";")
        statements.append("COMMIT;")
        self.query("\n".join(statements), expect_rows=False)

    def recover(self, table, plan, run_id, stage_time):
        info = self.inspect(table)
        parent = plan["parent"]
        if info is None:
            if parent is not None:
                raise ValueError("planned snapshot table is missing")
            return None
        metadata = info["metadata"]
        head = metadata.get("current-snapshot-id")
        if head is None or head == -1:
            if parent is not None:
                raise ValueError("planned parent snapshot is missing")
            return None
        snapshots = {str(item["snapshot-id"]): item for item in metadata.get("snapshots", [])}
        cursor = str(head)
        ancestry = []
        while cursor != parent:
            if cursor not in snapshots:
                raise ValueError("snapshot ancestry is missing")
            item = snapshots[cursor]
            ancestry.append(item)
            previous = item.get("parent-snapshot-id")
            cursor = str(previous) if previous is not None else None
            if cursor is None and parent is not None:
                raise ValueError("planned parent is not an ancestor")
        ancestry.reverse()
        if not ancestry:
            return None
        first = ancestry[0]
        has_marker = False
        for snapshot in ancestry:
            for row in self.rows(table, str(snapshot["snapshot-id"]), info):
                if row["stage_run_id"] == run_id and timestamp(row["stage_ingested_at"]) == stage_time:
                    has_marker = True
                if row["valid_to"] is not None and timestamp(row["valid_to"]) == stage_time:
                    has_marker = True
        if not has_marker:
            return None
        want_close = bool(plan["closes"])
        if first.get("summary", {}).get("operation") != ("overwrite" if want_close else "append"):
            raise ValueError("unexpected snapshot after planned parent")
        if want_close and len(ancestry) < 2:
            raise ValueError("partial overwrite after planned parent")
        append = ancestry[1] if want_close else first
        if append.get("summary", {}).get("operation") != "append":
            raise ValueError("missing planned append snapshot")
        append_id = str(append["snapshot-id"])
        rows = self.rows(table, append_id, info)
        selected = [row for row in rows if row["stage_run_id"] == run_id and
                    timestamp(row["stage_ingested_at"]) == stage_time]
        expected = {(tuple(item["key"]), item["version"], item["hash"]) for item in plan["appends"]}
        actual = {(tuple(row[name] for name in KEY_COLUMNS[table]), row["version"], row["record_hash"])
                  for row in selected if row["is_current"]}
        if actual != expected or len(selected) != len(expected):
            if not selected and len(ancestry) == 1 and not want_close:
                return None
            raise ValueError("snapshot append does not match pending intent")
        overwrite_id = None
        if want_close:
            overwrite_id = str(first["snapshot-id"])
            closed = self.rows(table, overwrite_id, info)
            for entry in plan["closes"]:
                matching = [row for row in closed if tuple(row[k] for k in KEY_COLUMNS[table]) == tuple(entry["key"])
                            and row["version"] == entry["version"] and not row["is_current"]]
                if len(matching) != 1 or matching[0]["valid_to"] is None:
                    raise ValueError("snapshot close does not match pending intent")
                if timestamp(matching[0]["valid_to"]) != stage_time:
                    raise ValueError("snapshot close timestamp mismatch")
        return dict(state="complete", overwrite=overwrite_id, append=append_id, changed_count=len(plan["appends"]))
