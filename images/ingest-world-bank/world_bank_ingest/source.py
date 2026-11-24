"""World Bank request construction and page validation."""
import argparse
import math
import random
import re
import time
from urllib.parse import urlparse

import requests
import simplejson as json
import yaml


def load_scope(args, path="config/pipeline.yml"):
    with open(path, encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    for key, pattern in (("countries", r"[A-Z]{3}"), ("indicators", r"[A-Z0-9]+(?:\.[A-Z0-9]+)+")):
        values = config[key]
        if not isinstance(values, list) or not values or len(set(values)) != len(values):
            raise ValueError("invalid configuration")
        if any(not isinstance(value, str) or not re.fullmatch(pattern, value) for value in values):
            raise ValueError("invalid configuration")
    if any(type(config[key]) is not int for key in ("start_year", "end_year")) or config["start_year"] > config["end_year"]:
        raise ValueError("invalid configuration")
    scope = {}
    for key in ("countries", "indicators"):
        selected = getattr(args, key)
        if selected is None:
            scope[key] = config[key]
            continue
        values = [value.strip() for value in selected.split(",")]
        if not all(values) or len(set(values)) != len(values) or not set(values) <= set(config[key]):
            raise ValueError("invalid " + key)
        scope[key] = [value for value in config[key] if value in values]
    scope["start_year"] = args.start_year if args.start_year is not None else config["start_year"]
    scope["end_year"] = args.end_year if args.end_year is not None else config["end_year"]
    if not config["start_year"] <= scope["start_year"] <= scope["end_year"] <= config["end_year"]:
        raise ValueError("invalid year range")
    return scope


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion-id", required=True)
    parser.add_argument("--mode", choices=("extract", "replay"), default="extract")
    parser.add_argument("--source-ingestion-id")
    parser.add_argument("--countries")
    parser.add_argument("--indicators")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    args = parser.parse_args(argv)
    for value in (args.ingestion_id, args.source_ingestion_id):
        if value is not None and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value, flags=re.ASCII):
            parser.error("invalid ingestion ID")
    if args.mode == "replay":
        if not args.source_ingestion_id or args.source_ingestion_id == args.ingestion_id or any(
            getattr(args, field) is not None for field in ("countries", "indicators", "start_year", "end_year")
        ):
            parser.error("replay requires a different source ID and no scope overrides")
        scope = None
    else:
        if args.source_ingestion_id:
            parser.error("source ID only applies to replay")
        try:
            scope = load_scope(args)
        except (ValueError, KeyError, OSError, yaml.YAMLError) as error:
            parser.error(str(error))
    return args, scope


def api_base(test_url):
    if not test_url:
        return "https://api.worldbank.org/v2"
    parsed = urlparse(test_url)
    if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("invalid fixture API URL")
    return test_url.rstrip("/")


def requests_for(scope, base):
    countries = ";".join(scope["countries"])
    yield "observation", [
        (f"{base}/country/{countries}/indicator/{indicator}", {"date": f"{scope['start_year']}:{scope['end_year']}", "source": "2"})
        for indicator in scope["indicators"]
    ]
    yield "country", [(f"{base}/country/{countries}", {})]
    yield "indicator", [(f"{base}/indicator/{indicator}", {"source": "2"}) for indicator in scope["indicators"]]
    yield "income_level", [(f"{base}/incomelevel", {})]


def number(value):
    if type(value) is int:
        return value
    if isinstance(value, str) and value.isascii() and value.isdigit():
        return int(value)
    raise ValueError("invalid pagination field")


def validate_page(body, page, expected_pages, table):
    try:
        result = json.loads(body, use_decimal=True, allow_nan=False)
        if not isinstance(result, list) or len(result) != 2 or not isinstance(result[0], dict):
            raise ValueError("invalid response shape")
        meta, records = result
        actual, pages = number(meta["page"]), number(meta["pages"])
        if actual != page or pages < 0 or (expected_pages is not None and pages != expected_pages):
            raise ValueError("invalid pagination")
        if pages == 0:
            if page != 1 or number(meta["total"]) != 0 or records not in (None, []):
                raise ValueError("invalid empty response")
            records = []
        elif page > pages or not isinstance(records, list):
            raise ValueError("invalid page records")
        for record in records:
            if not isinstance(record, dict):
                raise ValueError("invalid record")
            if table == "observation":
                if "value" not in record:
                    raise ValueError("missing observation value")
                value = record["value"]
                if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float, __import__("decimal").Decimal)) or not math.isfinite(value)):
                    raise ValueError("invalid observation value")
        return pages, records
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("invalid response") from error


def fetch(session, url, params, page, log, sleep=time.sleep, jitter=random.uniform):
    query = dict(params, format="json", per_page=50, page=page)
    for retry in range(4):
        try:
            response = session.get(url, params=query, timeout=(5, 20), allow_redirects=False)
            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.exceptions.RetryError(f"HTTP {response.status_code}")
            response.raise_for_status()
            if 300 <= response.status_code < 400:
                raise ValueError("redirect rejected")
            return response.content, response.url
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.RetryError) as error:
            if retry == 3:
                raise ValueError("source retry limit exceeded") from error
            log("WARN", "request_retry", page=page, reason=type(error).__name__)
            sleep(jitter(0, min(4, 0.5 * 2 ** retry)))
    raise AssertionError("unreachable")
