from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import io
import unittest

import simplejson as json

from world_bank_ingest import raw
from world_bank_stage import input as raw_input, scd2


class Store:
    def __init__(self):
        self.data = {}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.data[Key] = Body

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.data[Key])}


def fixture(store):
    scope = dict(ingestion_id="unit", raw_date="2025-01-01", successful_attempt=1,
                 raw_status="complete", mode="extract", request_countries=["BRA"],
                 request_indicators=["SP.POP.TOTL"], start_year=2020, end_year=2021,
                 raw_ingested_at=datetime.now(timezone.utc))
    records = {
        "observation": [{"countryiso3code":"BRA", "indicator":{"id":"SP.POP.TOTL"}, "date":"2020", "value":Decimal("1.25")}],
        "country": [{"id":"BRA", "name":"Brazil", "incomeLevel":{"id":"UMC"}}],
        "indicator": [{"id":"SP.POP.TOTL", "name":"Population"}],
        "income_level": [{"id":"UMC", "value":"Upper middle income"}],
    }
    for table, rows in records.items():
        writer = raw.TableWriter(store, table, scope["raw_date"], "unit", 1)
        body = json.dumps([{"page": 1, "pages": 1}, rows], use_decimal=True).encode()
        writer.page(1, 1, body, rows, "https://example.org")
        writer.publish()
    return scope


class StageUnit(unittest.TestCase):
    def test_canonical_numbers_keys_and_timestamps(self):
        a = {"z": Decimal("1.200"), "a": [Decimal("-0.0"), None, "2025-01-01T03:00:00+03:00"]}
        b = {"a": [0, None, "2025-01-01T00:00:00Z"], "z": Decimal("1.2")}
        self.assertEqual(raw_input.canonical(a), raw_input.canonical(b))
        self.assertEqual(raw_input.canonical(Decimal("1E+6")), "1000000")

    def test_raw_keys_lineage_and_corruption(self):
        store = Store()
        row = fixture(store)
        records = raw_input.read_extract(store, row)
        self.assertEqual(set(records), set(raw.TABLES))
        self.assertEqual(records["observation"][("BRA", "SP.POP.TOTL", 2020)]["value"], Decimal("1.25"))
        self.assertIn(("BRA",), records["country"])
        self.assertIn(("SP.POP.TOTL",), records["indicator"])
        self.assertIn(("UMC",), records["income_level"])
        key = next(k for k in store.data if "/observation/" in k and "/records/" in k)
        batch = json.loads(store.data[key], use_decimal=True)
        batch[0]["raw_record_index"] = 9
        store.data[key] = json.dumps(batch, use_decimal=True).encode()
        with self.assertRaises(ValueError):
            raw_input.read_extract(store, row)

    def test_missing_indicator_request_fails_extract_and_freshness(self):
        store = Store()
        row = fixture(store)
        row["request_indicators"].append("NY.GDP.MKTP.CD")
        row["raw_ingested_at"] = datetime.max.replace(tzinfo=timezone.utc)

        with self.assertRaisesRegex(ValueError, "incomplete source request listing"):
            raw_input.read_extract(store, row)

        older_row = dict(row, request_indicators=["SP.POP.TOTL"])
        candidates = raw_input.read_extract(store, older_row)
        with self.assertRaisesRegex(ValueError, "incomplete source request listing"):
            raw_input.filter_fresh(store, candidates, [row])

        for table in ("observation", "indicator"):
            writer = raw.TableWriter(store, table, row["raw_date"], row["ingestion_id"], 1)
            first = ({"countryiso3code": "BRA", "indicator": {"id": "SP.POP.TOTL"},
                      "date": "2020", "value": Decimal("1.25")}
                     if table == "observation" else {"id": "SP.POP.TOTL", "name": "Population"})
            # A complete request can legitimately return zero records.
            for ordinal, records in ((1, [first]), (2, [])):
                body = json.dumps([{"page": 1, "pages": 1}, records], use_decimal=True).encode()
                writer.page(ordinal, 1, body, records, "https://example.org")
            writer.publish()
            if table == "observation":
                with self.assertRaisesRegex(ValueError, "incomplete source request listing"):
                    raw_input.read_extract(store, row)

        complete = raw_input.read_extract(store, row)
        self.assertEqual(len(complete["observation"]), 1)
        self.assertEqual(len(complete["indicator"]), 1)
        self.assertEqual(raw_input.filter_fresh(store, complete, [row]), complete)

    def test_decimal_range(self):
        row = fixture(Store())
        base = {"countryiso3code":"BRA", "indicator":{"id":"SP.POP.TOTL"}, "date":"2020"}
        _, valid = raw_input._business("observation", dict(base, value=Decimal("99999999999999999999.999999999999999999")), row)
        self.assertIsNotNone(valid["value"])
        for value in (Decimal("1E20"), Decimal("1E-19")):
            with self.assertRaises(ValueError):
                raw_input._business("observation", dict(base, value=value), row)
        self.assertIsNone(raw_input._business("observation", dict(base, value=None), row)[1]["value"])

    def test_full_history_validity(self):
        first = dict(country_iso3="BRA", indicator_code="SP.POP.TOTL", year=2020,
                     version=1, valid_from="2025-01-01T00:00:00+00:00",
                     valid_to="2025-01-02T00:00:00+00:00", is_current=False, record_hash="a")
        second = dict(first, version=2, valid_from="2025-01-02T00:00:00+00:00",
                      valid_to="2025-01-03T00:00:00+00:00", record_hash="b")
        third = dict(first, version=3, valid_from="2025-01-03T00:00:00+00:00",
                     valid_to=None, is_current=True, record_hash="a")
        scd2.validate_history("observation", [third, first, second])
        scd2.validate_history("observation", [])
        invalid = [
            [first, second],
            [first, second, third, dict(third, version=4)],
            [first, dict(second, version=1), third],
            [first, dict(second, valid_from=first["valid_from"]), third],
            [dict(first, valid_to="2025-01-02T12:00:00+00:00"), second, third],
            [dict(first, valid_to=first["valid_from"]), second, third],
            [dict(first, valid_to=None), second, third],
            [first, second, dict(third, valid_to="2025-01-04T00:00:00+00:00")],
            [first, dict(second, record_hash="a"), third],
            [first, dict(second, valid_from="2025-01-02T00:00:00"), third],
            [dict(first, version=0), second, third],
        ]
        for rows in invalid:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                scd2.validate_history("observation", rows)

    def test_scd2_omission_null_update_and_later_reservation(self):
        t1 = datetime(2025, 1, 1, tzinfo=timezone.utc)
        t2 = datetime(2025, 1, 2, tzinfo=timezone.utc)
        key = ("BRA", "SP.POP.TOTL", 2020)
        old = dict(country_iso3="BRA", indicator_code="SP.POP.TOTL", year=2020,
                   valid_from=t1.isoformat(), version=1, record_hash="old")
        closes, appends = scd2.plan("observation", {key: {"record_hash":"new"}}, [old], t2)
        self.assertEqual(closes, [{"key":list(key), "version":1}])
        self.assertEqual(appends[0]["version"], 2)
        self.assertEqual(scd2.plan("observation", {}, [old], t2), ([], []))
        self.assertEqual(scd2.plan("observation", {key: {"record_hash":"old"}}, [old], t2), ([], []))
        self.assertEqual(scd2.plan("observation", {key: {"record_hash":"new"}}, [old], t1), ([], []))
        with self.assertRaises(ValueError):
            scd2.plan("observation", {key: {"record_hash":"new"}}, [old, old], t2)


if __name__ == "__main__":
    unittest.main()
