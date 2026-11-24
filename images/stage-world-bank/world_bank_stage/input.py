"""Validate published raw artifacts and prepare source records for stage."""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import re

BUCKET = "lakehouse-raw"
TABLES = ("observation", "country", "indicator", "income_level")
PROVENANCE = (
    "raw_response_key", "raw_record_index", "response_sha256", "raw_object_key",
    "source_url", "api_page", "raw_ingested_at", "ingestion_id",
)
_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")


def timestamp(value):
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value):
        raise ValueError("invalid source timestamp")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError as error:
        raise ValueError("invalid source timestamp") from error


def canonical(value):
    """Return sorted compact JSON with exact normalized numbers and UTC timestamps."""
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if isinstance(value, (int, Decimal)):
        decimal = Decimal(value)
        if not decimal.is_finite():
            raise ValueError("nonfinite number")
        if decimal == 0:
            return "0"
        result = format(decimal, "f")
        return result.rstrip("0").rstrip(".") if "." in result else result
    if isinstance(value, str):
        if _TIMESTAMP.fullmatch(value):
            try:
                value = timestamp(value).isoformat().replace("+00:00", "Z")
            except ValueError:
                pass
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, list):
        return "[" + ",".join(canonical(item) for item in value) + "]"
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("invalid object key")
        return "{" + ",".join(json.dumps(key, ensure_ascii=False) + ":" + canonical(value[key]) for key in sorted(value)) + "}"
    raise ValueError("unsupported source value")


def _loads(body):
    def invalid_constant(value):
        raise ValueError("invalid JSON number")

    try:
        return json.loads(body, parse_float=Decimal, parse_constant=invalid_constant)
    except (ValueError, UnicodeDecodeError, TypeError) as error:
        raise ValueError("invalid JSON artifact") from error


def _same_json(left, right):
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, (int, Decimal)) and isinstance(right, (int, Decimal)):
        return left == right
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(_same_json(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(_same_json(a, b) for a, b in zip(left, right))
    return left == right


def _get(s3, key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def _integer(value):
    if type(value) is int:
        return value
    if isinstance(value, str) and value.isascii() and value.isdigit():
        return int(value)
    raise ValueError("invalid integer")


def _required_string(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("missing business key")
    return value


def _optional_string(value):
    if value is not None and not isinstance(value, str):
        raise ValueError("invalid descriptive field")
    return value


def _business(table, payload, row):
    if table == "observation":
        country = _required_string(payload.get("countryiso3code"))
        indicator = payload.get("indicator")
        if not isinstance(indicator, dict):
            raise ValueError("missing indicator")
        code = _required_string(indicator.get("id"))
        year = _integer(payload.get("date"))
        if country not in row["request_countries"] or code not in row["request_indicators"] or not row["start_year"] <= year <= row["end_year"]:
            raise ValueError("observation outside requested scope")
        if "value" not in payload:
            raise ValueError("missing observation value")
        value = payload["value"]
        if value is not None:
            if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
                raise ValueError("invalid observation value")
            value = Decimal(value)
            if not value.is_finite():
                raise ValueError("invalid observation value")
            parts = value.as_tuple()
            digits = list(parts.digits)
            exponent = parts.exponent
            while len(digits) > 1 and digits[-1] == 0:
                digits.pop()
                exponent += 1
            scale = max(-exponent, 0) if value else 0
            integer_digits = max(len(digits) + exponent, 0) if value else 0
            if scale > 18 or integer_digits > 20:
                raise ValueError("observation value exceeds decimal precision")
        return (country, code, year), dict(country_iso3=country, indicator_code=code, year=year, value=value)
    key = _required_string(payload.get("id"))
    if table == "country":
        if key not in row["request_countries"]:
            raise ValueError("country outside requested scope")
        income = payload.get("incomeLevel")
        if income is not None and not isinstance(income, dict):
            raise ValueError("invalid income level")
        return (key,), dict(country_iso3=key, country_name=_optional_string(payload.get("name")), income_level_id=_optional_string(income.get("id") if income else None))
    if table == "indicator":
        if key not in row["request_indicators"]:
            raise ValueError("indicator outside requested scope")
        return (key,), dict(indicator_code=key, indicator_name=_optional_string(payload.get("name")))
    return (key,), dict(income_level_id=key, income_level_name=_optional_string(payload.get("value")))


def _response(body, expected_page, previous_pages):
    value = _loads(body)
    if not isinstance(value, list) or len(value) != 2 or not isinstance(value[0], dict):
        raise ValueError("invalid response shape")
    meta, records = value
    try:
        page, pages = _integer(meta["page"]), _integer(meta["pages"])
    except KeyError as error:
        raise ValueError("missing page metadata") from error
    if page != expected_page or pages < 0 or (previous_pages is not None and previous_pages != pages):
        raise ValueError("invalid response pagination")
    if pages == 0:
        if page != 1 or _integer(meta.get("total")) != 0 or records not in (None, []):
            raise ValueError("invalid empty response")
        records = []
    elif page > pages or not isinstance(records, list):
        raise ValueError("invalid response records")
    if any(not isinstance(record, dict) or set(record).intersection(PROVENANCE) for record in records):
        raise ValueError("invalid response record")
    return pages, records


def read_extract(s3, row):
    """Read and validate every table from one complete extract row."""
    if row.get("mode") != "extract" or row.get("raw_status") != "complete":
        raise ValueError("extract is not complete")
    ingestion_id = _required_string(row.get("ingestion_id"))
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", ingestion_id, flags=re.ASCII):
        raise ValueError("invalid source ID")
    raw_date = str(row.get("raw_date"))
    try:
        datetime.strptime(raw_date, "%Y-%m-%d")
    except ValueError as error:
        raise ValueError("invalid raw date") from error
    attempt = row.get("successful_attempt")
    if type(attempt) is not int or attempt < 1:
        raise ValueError("invalid source attempt")
    output = {}
    for table in TABLES:
        root = f"world-bank/{table}/date={raw_date}/ingestion_id={ingestion_id}/"
        prefix = f"{root}attempt={attempt}/"
        manifest = _loads(_get(s3, root + "manifest.json"))
        expected = dict(format_version=1, table=table, ingestion_id=ingestion_id,
                        raw_date=raw_date, successful_attempt=attempt)
        if not isinstance(manifest, dict) or any(manifest.get(key) != value for key, value in expected.items()):
            raise ValueError("invalid manifest identity")
        responses = manifest.get("responses")
        record_keys = manifest.get("record_keys")
        if not isinstance(responses, list) or not responses or not isinstance(record_keys, list):
            raise ValueError("invalid manifest listing")
        artifacts = set()
        sources = {}
        expected_records = set()
        request_number = 0
        page_number = 0
        page_count = None
        for entry in responses:
            if not isinstance(entry, dict):
                raise ValueError("invalid response listing")
            key = entry.get("key")
            pattern = re.escape(prefix) + r"responses/request_(\d{3,})_page_(\d{3,})\.json"
            match = re.fullmatch(pattern, key) if isinstance(key, str) else None
            if not match or key in artifacts:
                raise ValueError("invalid response key")
            request, page = map(int, match.groups())
            if request == request_number + 1:
                if request_number and page_number != max(page_count, 1):
                    raise ValueError("incomplete request")
                request_number, page_number, page_count = request, 0, None
            if request != request_number or page != page_number + 1:
                raise ValueError("missing response page")
            page_number = page
            digest = entry.get("sha256")
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError("invalid response hash")
            if type(entry.get("api_page")) is not int or entry["api_page"] != page:
                raise ValueError("invalid response page")
            _required_string(entry.get("source_url"))
            timestamp(entry.get("raw_ingested_at"))
            body = _get(s3, key)
            if hashlib.sha256(body).hexdigest() != digest:
                raise ValueError("response hash mismatch")
            page_count, body_records = _response(body, page, page_count)
            artifacts.add(key)
            sources[key] = (entry, body_records)
            expected_records.update((key, index) for index in range(len(body_records)))
        if page_number != max(page_count, 1):
            raise ValueError("incomplete final request")
        expected_requests = len(row["request_indicators"]) if table in ("observation", "indicator") else 1
        if request_number != expected_requests:
            raise ValueError("incomplete source request listing")
        candidates = {}
        seen = set()
        for batch_number, key in enumerate(record_keys, 1):
            if not isinstance(key, str) or key != f"{prefix}records/{batch_number:05d}.json" or key in artifacts:
                raise ValueError("invalid record key")
            artifacts.add(key)
            batch = _loads(_get(s3, key))
            if not isinstance(batch, list) or not 1 <= len(batch) <= 100:
                raise ValueError("invalid record batch")
            for record in batch:
                if not isinstance(record, dict) or not set(PROVENANCE).issubset(record):
                    raise ValueError("missing record provenance")
                response_key = record["raw_response_key"]
                index = record["raw_record_index"]
                if not isinstance(response_key, str) or response_key not in sources or type(index) is not int or index < 0:
                    raise ValueError("invalid record reference")
                entry, body_records = sources[response_key]
                record_id = (response_key, index)
                if index >= len(body_records) or record_id in seen:
                    raise ValueError("duplicate or invalid record index")
                expected_lineage = dict(raw_response_key=response_key, raw_record_index=index,
                                        response_sha256=entry["sha256"], raw_object_key=key,
                                        source_url=entry["source_url"], api_page=entry["api_page"],
                                        raw_ingested_at=entry["raw_ingested_at"], ingestion_id=ingestion_id)
                if any(type(record[name]) is not type(value) or record[name] != value for name, value in expected_lineage.items()):
                    raise ValueError("record provenance mismatch")
                payload = {name: value for name, value in record.items() if name not in PROVENANCE}
                if not _same_json(payload, body_records[index]):
                    raise ValueError("record body mismatch")
                seen.add(record_id)
                key_tuple, business = _business(table, payload, row)
                api_payload = canonical(payload)
                digest = hashlib.sha256(api_payload.encode("utf-8")).hexdigest()
                order = (timestamp(record["raw_ingested_at"]), ingestion_id, response_key, index)
                candidate = dict(business, api_payload=api_payload, record_hash=digest,
                                 source_order=order, **expected_lineage)
                prior = candidates.get(key_tuple)
                if prior and prior["record_hash"] != digest:
                    raise ValueError("conflicting source values for business key")
                if prior is None or prior["source_order"] < order:
                    candidates[key_tuple] = candidate
        if seen != expected_records:
            raise ValueError("response records missing from batches")
        output[table] = candidates
    return output


def filter_fresh(s3, candidates, extract_rows):
    """Discard candidates superseded by a later successful extract's same key."""
    latest = {table: {key: value["source_order"] for key, value in records.items()}
              for table, records in candidates.items()}
    for row in extract_rows:
        if row.get("mode") != "extract" or row.get("raw_status") != "complete":
            continue
        relevant = []
        for table, records in candidates.items():
            for key, candidate in records.items():
                if row["raw_ingested_at"] < candidate["source_order"][0]:
                    continue
                if table == "observation" and (key[0] not in row["request_countries"] or key[1] not in row["request_indicators"] or not row["start_year"] <= key[2] <= row["end_year"]):
                    continue
                if table == "country" and key[0] not in row["request_countries"]:
                    continue
                if table == "indicator" and key[0] not in row["request_indicators"]:
                    continue
                relevant.append((table, key))
        if not relevant:
            continue
        source = read_extract(s3, row)
        for table, key in relevant:
            found = source[table].get(key)
            if found and found["source_order"] > latest[table][key]:
                latest[table][key] = found["source_order"]
    return {table: {key: value for key, value in records.items()
                    if value["source_order"] >= latest[table][key]}
            for table, records in candidates.items()}
