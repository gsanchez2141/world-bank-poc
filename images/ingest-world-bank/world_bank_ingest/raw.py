"""Raw response storage, batching and committed artifact verification."""
from datetime import datetime, timezone
import hashlib
import re

import simplejson as json

BUCKET = "lakehouse-raw"
TABLES = ("observation", "country", "indicator", "income_level")
RESERVED = {"raw_response_key", "raw_record_index", "response_sha256", "raw_object_key", "source_url", "api_page", "raw_ingested_at", "ingestion_id"}


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def base(table, raw_date, ingestion_id):
    return f"world-bank/{table}/date={raw_date}/ingestion_id={ingestion_id}/"


def put(s3, key, body):
    s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")


class TableWriter:
    def __init__(self, s3, table, raw_date, ingestion_id, attempt):
        self.s3 = s3
        self.root = base(table, raw_date, ingestion_id)
        self.attempt_prefix = f"{self.root}attempt={attempt}/"
        self.buffer = []
        self.manifest = dict(format_version=1, table=table, ingestion_id=ingestion_id,
                             raw_date=str(raw_date), successful_attempt=attempt, responses=[], record_keys=[])
        self.latest = None

    def page(self, ordinal, page, body, records, url):
        key = f"{self.attempt_prefix}responses/request_{ordinal:03d}_page_{page:03d}.json"
        timestamp = now()
        digest = hashlib.sha256(body).hexdigest()
        put(self.s3, key, body)
        self.manifest["responses"].append(dict(key=key, sha256=digest, source_url=url, api_page=page, raw_ingested_at=timestamp))
        self.latest = timestamp
        for index, record in enumerate(records):
            if RESERVED.intersection(record):
                raise ValueError("source record contains reserved provenance field")
            self.buffer.append(dict(record, raw_response_key=key, raw_record_index=index, response_sha256=digest,
                                    source_url=url, api_page=page, raw_ingested_at=timestamp,
                                    ingestion_id=self.manifest["ingestion_id"]))
            if len(self.buffer) == 100:
                self.flush()

    def flush(self):
        if not self.buffer:
            return
        key = f"{self.attempt_prefix}records/{len(self.manifest['record_keys']) + 1:05d}.json"
        for record in self.buffer:
            record["raw_object_key"] = key
        put(self.s3, key, json.dumps(self.buffer, use_decimal=True, allow_nan=False, separators=(",", ":")).encode())
        self.manifest["record_keys"].append(key)
        self.buffer.clear()

    def publish(self):
        self.flush()
        put(self.s3, self.root + "manifest.json", json.dumps(self.manifest, separators=(",", ":")).encode())


def verify(s3, ingestion_id, raw_date, attempt, hash_bodies):
    for table in TABLES:
        root = base(table, raw_date, ingestion_id)
        prefix = f"{root}attempt={attempt}/"
        data = json.loads(s3.get_object(Bucket=BUCKET, Key=root + "manifest.json")["Body"].read())
        if not isinstance(data, dict) or any(data.get(k) != v for k, v in {
            "format_version": 1, "table": table, "ingestion_id": ingestion_id,
            "raw_date": str(raw_date), "successful_attempt": attempt
        }.items()):
            raise ValueError("invalid manifest identity")
        responses, records = data.get("responses"), data.get("record_keys")
        if not isinstance(responses, list) or not responses or not isinstance(records, list):
            raise ValueError("invalid manifest listing")
        keys = set()
        for response in responses:
            if not isinstance(response, dict):
                raise ValueError("invalid response manifest")
            key = response.get("key")
            if (not isinstance(key, str) or not key.startswith(prefix + "responses/") or
                not re.fullmatch(r"[0-9a-f]{64}", str(response.get("sha256"))) or
                not isinstance(response.get("source_url"), str) or
                type(response.get("api_page")) is not int or response["api_page"] < 1 or
                not isinstance(response.get("raw_ingested_at"), str)):
                raise ValueError("invalid response manifest")
            if key in keys:
                raise ValueError("duplicate artifact")
            keys.add(key)
            if hash_bodies:
                body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                if hashlib.sha256(body).hexdigest() != response["sha256"]:
                    raise ValueError("response hash mismatch")
            else:
                s3.head_object(Bucket=BUCKET, Key=key)
        for key in records:
            if not isinstance(key, str) or not key.startswith(prefix + "records/") or key in keys:
                raise ValueError("invalid record manifest")
            keys.add(key)
            s3.head_object(Bucket=BUCKET, Key=key)
