"""Plan new versions without changing Iceberg."""
from collections import defaultdict
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



def validate_history(table, rows):
    """Reject invalid SCD2 intervals and repeated adjacent versions."""
    grouped = defaultdict(list)
    for row in rows:
        key = tuple(row.get(column) for column in KEY_COLUMNS[table])
        if any(value is None or isinstance(value, str) and not value for value in key):
            raise ValueError("invalid history business key")
        version = row.get("version")
        if type(version) is not int or version < 1:
            raise ValueError("invalid history version")
        if type(row.get("is_current")) is not bool:
            raise ValueError("invalid current marker")
        if not isinstance(row.get("record_hash"), str) or not row["record_hash"]:
            raise ValueError("invalid history hash")
        try:
            start = timestamp(row["valid_from"])
            end = timestamp(row["valid_to"]) if row.get("valid_to") is not None else None
            if start.tzinfo is None or (end is not None and end.tzinfo is None):
                raise ValueError("history timestamps must include timezone")
        except (KeyError, TypeError, AttributeError, OverflowError) as error:
            raise ValueError("invalid history timestamp") from error
        if row["is_current"] != (end is None) or (end is not None and end <= start):
            raise ValueError("invalid history validity interval")
        grouped[key].append((version, start, end, row["record_hash"], row["is_current"]))

    for versions in grouped.values():
        versions.sort(key=lambda item: item[0])
        if sum(item[4] for item in versions) != 1:
            raise ValueError("history must have exactly one current version")
        for previous, current in zip(versions, versions[1:]):
            if (previous[0] == current[0] or previous[1] >= current[1] or previous[2] is None or
                    previous[2] > current[1]):
                raise ValueError("invalid or overlapping history")
            if previous[3] == current[3]:
                raise ValueError("adjacent history versions repeat a hash")


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
