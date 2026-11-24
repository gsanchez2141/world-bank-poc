"""Validate trigger inputs and reserve a pipeline run in the warehouse."""
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re

import psycopg2
from psycopg2.extras import RealDictCursor
import yaml

ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", re.ASCII)
SCOPE = ("countries", "indicators", "start_year", "end_year")
ALLOWED = set(SCOPE) | {"ingestion_id", "mode", "source_ingestion_id", "abandon_previous"}


def valid_id(value):
    return isinstance(value, str) and bool(ID_PATTERN.fullmatch(value))


def fallback_id(run_id):
    if not isinstance(run_id, str) or not run_id:
        raise ValueError("missing Airflow run ID")
    return "airflow_" + hashlib.sha256(run_id.encode()).hexdigest()


def ingestion_id(conf, run_id):
    if "ingestion_id" in conf:
        if not valid_id(conf["ingestion_id"]):
            raise ValueError("invalid ingestion ID")
        return conf["ingestion_id"]
    return fallback_id(run_id)


def connect():
    return psycopg2.connect(host="postgres", port=5432, dbname="warehouse", user="postgres", password="postgres",
                            cursor_factory=RealDictCursor)


def load_run(connection, identifier):
    with connection.cursor() as cursor:
        cursor.execute("SELECT * FROM meta.pipeline_run WHERE ingestion_id=%s", (identifier,))
        return cursor.fetchone()


def validate_conf(conf, config, existing=None, source=None):
    if not isinstance(conf, dict) or set(conf) - ALLOWED:
        raise ValueError("invalid trigger configuration")
    if "abandon_previous" in conf and type(conf["abandon_previous"]) is not bool:
        raise ValueError("abandon_previous must be a boolean")
    mode = conf.get("mode", existing["mode"] if existing else "extract")
    if mode not in ("extract", "replay", "reprocess_commit", "rebuild"):
        raise ValueError("unsupported pipeline mode")
    source_id = conf.get("source_ingestion_id", existing["source_ingestion_id"] if existing else None)
    identifier = conf.get("ingestion_id")
    if source_id is not None and (not valid_id(source_id) or source_id == identifier):
        raise ValueError("invalid source ingestion ID")
    if existing and existing.get("abandoned_at") is not None:
        raise ValueError("abandoned run cannot be resumed")
    if existing and (mode != existing["mode"] or source_id != existing["source_ingestion_id"]):
        raise ValueError("run identity conflicts")
    if "abandon_previous" in conf and (existing or mode != "extract"):
        raise ValueError("abandon_previous applies only to a new extract")
    if mode != "extract":
        if any(field in conf for field in SCOPE):
            raise ValueError("scope overrides are unavailable for this mode")
        if mode != "rebuild" and (not source_id or not source):
            raise ValueError("source run not found")
        if mode == "rebuild" and source_id and (not source or source["mode"] not in ("extract", "replay") or source["stage_status"] != "complete"):
            raise ValueError("rebuild audit source is not stage-complete")
        if mode == "replay" and (source["mode"] != "extract" or source["raw_status"] != "complete"):
            raise ValueError("replay source extract is not complete")
        if mode == "reprocess_commit" and (source["mode"] not in ("extract", "replay") or source["stage_status"] != "complete"):
            raise ValueError("source stage commit is not complete")
        if mode == "rebuild":
            scope = {field: None for field in SCOPE}
        else:
            scope = {field: source["request_" + field] if field in ("countries", "indicators") else source[field] for field in SCOPE}
    else:
        if source_id is not None:
            raise ValueError("extract cannot name a source")
        scope = {}
        for field in ("countries", "indicators"):
            default = existing["request_" + field] if existing else config[field]
            selected = conf.get(field, default)
            if (not isinstance(selected, list) or not selected or
                    any(not isinstance(value, str) for value in selected) or
                    len(selected) != len(set(selected)) or not set(selected) <= set(config[field])):
                raise ValueError("invalid " + field)
            scope[field] = [value for value in config[field] if value in selected]
        for field in ("start_year", "end_year"):
            scope[field] = conf.get(field, existing[field] if existing else config[field])
            if type(scope[field]) is not int:
                raise ValueError("invalid year")
        if not config["start_year"] <= scope["start_year"] <= scope["end_year"] <= config["end_year"]:
            raise ValueError("invalid year range")
    if existing:
        previous = {field: existing["request_" + field] if field in ("countries", "indicators") else existing[field] for field in SCOPE}
        if previous != scope:
            raise ValueError("run scope conflicts")
    return mode, source_id, scope


def gate(**context):
    conf = context["dag_run"].conf
    if not isinstance(conf, dict):
        raise ValueError("invalid trigger configuration")
    identifier = ingestion_id(conf, context["dag_run"].run_id)
    conf = dict(conf, ingestion_id=identifier)
    config_path = os.environ.get("WORLD_BANK_CONFIG_PATH", "/opt/world-bank/config/pipeline.yml")
    schema_path = os.environ.get("WORLD_BANK_SCHEMA_PATH", "/opt/world-bank/pipeline_run.sql")
    config = yaml.safe_load(Path(config_path).read_text())
    connection = connect()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('world_bank_pipeline_run_ddl'))")
                cursor.execute(Path(schema_path).read_text())
                cursor.execute("ALTER TABLE meta.pipeline_run ADD COLUMN IF NOT EXISTS abandoned_at timestamptz")
                cursor.execute("ALTER TABLE meta.pipeline_run ADD COLUMN IF NOT EXISTS abandoned_by_ingestion_id text")
                cursor.execute("ALTER TABLE meta.pipeline_run ADD COLUMN IF NOT EXISTS rebuild_snapshot_ids jsonb")
                cursor.execute("ALTER TABLE meta.pipeline_run ADD COLUMN IF NOT EXISTS rebuild_pinned_at timestamptz")
        with connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('world_bank_pipeline_gate'))")
                existing = load_run(connection, identifier)
                source_id = conf.get("source_ingestion_id", existing["source_ingestion_id"] if existing else None)
                source = load_run(connection, source_id) if source_id and valid_id(source_id) else None
                mode, source_id, scope = validate_conf(conf, config, existing, source)
                if existing:
                    return identifier
                if mode == "extract":
                    cursor.execute("""SELECT ingestion_id FROM meta.pipeline_run
                        WHERE abandoned_at IS NULL AND curated_status <> 'complete'
                        AND mode IN ('extract','replay','reprocess_commit','rebuild') FOR UPDATE""")
                    blockers = cursor.fetchall()
                    if blockers and not conf.get("abandon_previous", False):
                        raise ValueError("another pipeline run is incomplete")
                    if blockers:
                        cursor.execute("""UPDATE meta.pipeline_run SET abandoned_at=clock_timestamp(),
                            abandoned_by_ingestion_id=%s WHERE ingestion_id=ANY(%s)""",
                            (identifier, [row["ingestion_id"] for row in blockers]))
                raw_date = source["raw_date"] if mode == "replay" else (None if mode == "rebuild" else datetime.now(timezone.utc).date())
                cursor.execute("""INSERT INTO meta.pipeline_run
                    (ingestion_id,mode,source_ingestion_id,request_countries,request_indicators,start_year,end_year,raw_date)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (identifier, mode, source_id, scope["countries"], scope["indicators"],
                     scope["start_year"], scope["end_year"], raw_date))
        return identifier
    finally:
        connection.close()
