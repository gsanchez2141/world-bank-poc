"""Plan new versions without changing Iceberg."""
from datetime import datetime

KEY_COLUMNS = {
    "observation": ("country_iso3", "indicator_code", "year"),
    "country": ("country_iso3",),
    "indicator": ("indicator_code",),
    "income_level": ("income_level_id",),
}


def timestamp(value):
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00").replace(" ", "T"))


def plan(table, candidates, current, stage_time):
    columns = KEY_COLUMNS[table]
    by_key = {}
    for row in current:
        key = tuple(row[column] for column in columns)
        if key in by_key:
            raise ValueError("multiple current versions for key")
        by_key[key] = row
    closes, appends = [], []
    for key, candidate in candidates.items():
        previous = by_key.get(key)
        if previous and timestamp(previous["valid_from"]) >= stage_time:
            continue
        if previous and previous["record_hash"] == candidate["record_hash"]:
            continue
        version = 1
        if previous:
            version = previous["version"] + 1
            closes.append(dict(key=list(key), version=previous["version"]))
        appends.append(dict(key=list(key), version=version, hash=candidate["record_hash"]))
    return closes, appends
