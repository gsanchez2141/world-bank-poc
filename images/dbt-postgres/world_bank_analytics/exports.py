"""Typed, deterministic report serialization and publication."""
import csv
from decimal import Decimal
import io
import os
from pathlib import Path
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

TEXT = pa.string()
YEAR = pa.int32()
FACT = pa.decimal128(38, 18)
COMPUTED = pa.decimal128(38, 12)
SPECS = {
    "vw_missing_or_incomplete": (
        [("country_iso3", TEXT), ("indicator_code", TEXT), ("year", YEAR), ("value", FACT), ("reason", TEXT)],
        ("country_iso3", "indicator_code", "year"),
    ),
    "vw_yoy_change": (
        [("country_iso3", TEXT), ("indicator_code", TEXT), ("year", YEAR), ("value", FACT),
         ("prior_value", FACT), ("yoy_change_pct", COMPUTED), ("reason", TEXT)],
        ("country_iso3", "indicator_code", "year"),
    ),
    "vw_latest_year_comparison": (
        [("country_iso3", TEXT), ("indicator_code", TEXT), ("year", YEAR), ("value", FACT)],
        ("country_iso3", "indicator_code", "year"),
    ),
    "vw_income_group_year": (
        [("income_level_id", TEXT), ("income_level_name", TEXT), ("year", YEAR), ("indicator_code", TEXT),
         ("aggregation_method", TEXT), ("value", COMPUTED), ("expected_country_count", YEAR),
         ("available_country_count", YEAR), ("coverage_pct", COMPUTED)],
        ("income_level_id", "indicator_code", "year"),
    ),
}


def render(rows_by_view):
    artifacts = {}
    for name, (columns, order) in SPECS.items():
        rows = sorted(rows_by_view[name], key=lambda row: tuple(row[column] for column in order))
        names = [column for column, _ in columns]
        text = io.StringIO(newline="")
        writer = csv.writer(text, lineterminator="\n")
        writer.writerow(names)
        for row in rows:
            writer.writerow(["" if row[column] is None else format(row[column], "f") if isinstance(row[column], Decimal) else str(row[column]) for column in names])
        schema = pa.schema([pa.field(column, kind) for column, kind in columns])
        table = pa.Table.from_pylist([{column: row[column] for column in names} for row in rows], schema=schema)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        artifacts[name] = (text.getvalue().encode("utf-8"), buffer.getvalue())
    return artifacts


def publish(s3, ingestion_id, stage_day, artifacts, directory):
    prefix = f"world-bank/reporting/date={stage_day}/ingestion_id={ingestion_id}/"
    for name in SPECS:
        csv_body, parquet_body = artifacts[name]
        for extension, body, content_type in (
            ("csv", csv_body, "text/csv; charset=utf-8"),
            ("parquet", parquet_body, "application/vnd.apache.parquet"),
        ):
            s3.put_object(Bucket="lakehouse-curated", Key=prefix + name + "." + extension,
                          Body=body, ContentType=content_type)
    destination = Path(directory)
    for name in SPECS:
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=destination, prefix=".report-", delete=False) as handle:
                temporary = handle.name
                handle.write(artifacts[name][0])
            os.replace(temporary, destination / (name + ".csv"))
            temporary = None
        finally:
            if temporary is not None:
                os.unlink(temporary)
