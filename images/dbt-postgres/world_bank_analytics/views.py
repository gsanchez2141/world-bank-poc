"""Create reporting views and atomically refresh their configured key grid."""
from argparse import Namespace
from pathlib import Path

from world_bank_ingest.source import load_scope


def refresh(connection, config_path):
    scope = load_scope(Namespace(countries=None, indicators=None, start_year=None, end_year=None), config_path)
    with connection.transaction():
        connection.execute(Path(__file__).with_name("reporting.sql").read_text())
        connection.execute("DELETE FROM meta.expected_key")
        connection.execute("""
            INSERT INTO meta.expected_key(country_iso3, indicator_code, year)
            SELECT country, indicator, years.year
            FROM unnest(%s::text[]) AS country
            CROSS JOIN unnest(%s::text[]) AS indicator
            CROSS JOIN (SELECT DISTINCT year FROM curated.dim_year) AS years
        """, (scope["countries"], scope["indicators"]))
