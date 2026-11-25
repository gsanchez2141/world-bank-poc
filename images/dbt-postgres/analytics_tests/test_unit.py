import argparse
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from decimal import Decimal
import io
import os
import unittest
from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from world_bank_analytics import __main__ as analytics, exports


class ExportUnit(unittest.TestCase):
    def test_empty_schemas_and_exact_decimal_roundtrip(self):
        artifacts = exports.render({name: [] for name in exports.SPECS})
        for name, (csv_bytes, parquet_bytes) in artifacts.items():
            expected = pa.schema([pa.field(column, kind) for column, kind in exports.SPECS[name][0]])
            table = pq.read_table(io.BytesIO(parquet_bytes))
            self.assertEqual(table.schema, expected)
            self.assertEqual(table.num_rows, 0)
            self.assertEqual(csv_bytes.decode(), ",".join(expected.names) + "\n")

        rows = {name: [] for name in exports.SPECS}
        name = "vw_latest_year_comparison"
        precise = Decimal("12345678901234567890.123456789012345678")
        rows[name] = [
            dict(country_iso3="CAN", indicator_code="SP.POP.TOTL", year=2021, value=None),
            dict(country_iso3="BRA", indicator_code="SP.POP.TOTL", year=2021, value=precise),
        ]
        csv_bytes, parquet_bytes = exports.render(rows)[name]
        self.assertIn(b"12345678901234567890.123456789012345678", csv_bytes)
        self.assertLess(csv_bytes.index(b"BRA"), csv_bytes.index(b"CAN"))
        self.assertEqual(pq.read_table(io.BytesIO(parquet_bytes)).to_pylist()[0]["value"], precise)
        self.assertIsNone(pq.read_table(io.BytesIO(parquet_bytes)).to_pylist()[1]["value"])

    def test_partial_upload_does_not_complete(self):
        runs = MagicMock()
        runs.get.return_value = dict(curated_status="complete", export_status="pending")
        reporter = Mock()
        s3 = Mock()
        s3.put_object.side_effect = [{}, OSError("second upload failure")]
        source = {"stage_ingested_at":datetime(2025, 1, 2, tzinfo=timezone.utc)}
        with patch.object(analytics, "database", return_value="warehouse"), \
             patch.object(analytics, "clients", return_value=(s3, Mock(), Mock())), \
             patch.object(analytics, "Reporter", return_value=reporter), \
             patch.object(analytics, "Runs", return_value=runs), \
             patch.object(analytics, "select_stage", return_value=source), \
             patch.object(analytics.views, "refresh"), \
             patch.dict(os.environ, {"WORLD_BANK_ALERT_TOPIC_ARN":"local"}), \
             redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(analytics.process(argparse.Namespace(ingestion_id="run")), 1)
        self.assertEqual([call.args[0] for call in runs.export_status.call_args_list], ["pending", "failed"])
        self.assertEqual(s3.put_object.call_count, 2)
        reporter.failure.assert_called_once()


if __name__ == "__main__":
    unittest.main()
