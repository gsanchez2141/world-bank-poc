from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import argparse
import io
import os
import unittest
from unittest.mock import MagicMock, Mock, patch

from world_bank_dq import __main__ as quality, checks

NOW = datetime(2025, 1, 2, tzinfo=timezone.utc)
CONFIG = {"absolute_usd":Decimal("1"), "relative_fraction":Decimal("0.01"),
          "disappearance_jump_threshold":1}
SCOPE = dict(ingestion_id="current", mode="extract", raw_ingested_at=NOW,
             request_countries=["BRA"], request_indicators=[checks.POPULATION, checks.GDP, checks.PER_CAPITA],
             start_year=2020, end_year=2021)


def fact(indicator, value, country="BRA", year=2021, ingestion_id="old"):
    return dict(country_iso3=country, indicator_code=indicator, year=year, value=value,
                ingestion_id=ingestion_id, raw_ingested_at=NOW-timedelta(days=1),
                source_raw_ingested_at=NOW-timedelta(days=1))


class QualityUnit(unittest.TestCase):
    def test_config_validation(self):
        good = {"data_quality":{"gdp_per_capita":{"absolute_usd":"1","relative_fraction":"0.01"},
                                "disappearance_jump_threshold":1}}
        self.assertEqual(checks.validate_config(good), CONFIG)
        for bad in ("NaN", "-1", 1.5, True):
            candidate = {"data_quality":{"gdp_per_capita":{"absolute_usd":bad,"relative_fraction":"0.01"},
                                         "disappearance_jump_threshold":1}}
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                checks.validate_config(candidate)

    def test_population_and_gdp_tolerance_vintage(self):
        trio = [fact(checks.GDP, Decimal(1000)), fact(checks.POPULATION, Decimal(100)),
                fact(checks.PER_CAPITA, Decimal(11))]
        findings, diagnostic = checks.evaluate(SCOPE, trio, {}, [], CONFIG)
        self.assertFalse(any(item["severity"] == "error" for item in findings))
        self.assertEqual(diagnostic["gdp_checked"], 1)
        trio[2]["value"] = Decimal("11.0001")
        findings, _ = checks.evaluate(SCOPE, trio, {}, [], CONFIG)
        self.assertIn("gdp_per_capita_consistency", [item["check_name"] for item in findings])
        trio[1]["value"] = Decimal(0)
        findings, _ = checks.evaluate(SCOPE, trio, {}, [], CONFIG)
        self.assertIn("population_positive", [item["check_name"] for item in findings])
        trio[1]["value"] = None
        findings, diagnostic = checks.evaluate(SCOPE, trio, {}, [], CONFIG)
        self.assertFalse(any(item["severity"] == "error" for item in findings))
        self.assertEqual(diagnostic["gdp_skipped"], 1)
        trio[1]["value"] = Decimal(100)
        trio[2]["ingestion_id"] = "different"
        findings, diagnostic = checks.evaluate(SCOPE, trio, {}, [], CONFIG)
        self.assertEqual(diagnostic["gdp_checked"], 0)
        source = {("BRA", f["indicator_code"], 2021):{"value":f["value"]} for f in trio}
        findings, diagnostic = checks.evaluate(SCOPE, trio, source, [], CONFIG)
        self.assertEqual(diagnostic["gdp_checked"], 1)
        source[("BRA", checks.PER_CAPITA, 2021)]["value"] = Decimal(10)
        _, diagnostic = checks.evaluate(SCOPE, trio, source, [], CONFIG)
        self.assertEqual(diagnostic["gdp_checked"], 0)

    def test_scope_presence_and_prior_evidence(self):
        rows = [fact(checks.POPULATION, None, year=2020), fact(checks.GDP, 10),
                fact(checks.PER_CAPITA, 5, country="CAN"), fact(checks.GDP, 3, year=2019)]
        present = {("BRA", checks.POPULATION, 2020):{"value":None}}
        findings, _ = checks.evaluate(SCOPE, rows, present, [], CONFIG)
        warnings = [x for x in findings if x["check_name"] == "observation_disappeared"]
        self.assertEqual([x["business_key"] for x in warnings],
                         [dict(country_iso3="BRA", indicator_code=checks.GDP, year=2021)])
        newest = fact(checks.GDP, 99, ingestion_id="newer")
        newest["raw_ingested_at"] = NOW + timedelta(days=1)
        newest["source_raw_ingested_at"] = newest["raw_ingested_at"]
        older = fact(checks.GDP, 10)
        findings, _ = checks.evaluate(SCOPE, [newest], {}, [older], CONFIG)
        self.assertEqual(len([x for x in findings if x["check_name"] == "observation_disappeared"]), 1)

    def test_jump_requires_comparable_completed_baseline(self):
        baseline = dict(SCOPE, ingestion_id="baseline", raw_ingested_at=NOW-timedelta(days=1),
                        mode="extract", raw_status="complete", dq_status="complete", warning_count=0)
        self.assertEqual(checks.jump(SCOPE, [], 2, 1), (None, None))
        self.assertEqual(checks.jump(SCOPE, [baseline], 1, 1), (None, None))
        finding, payload = checks.jump(SCOPE, [baseline], 2, 1)
        self.assertEqual(finding["check_name"], "observation_disappearance_jump")
        self.assertEqual((payload["baseline_ingestion_id"], payload["increase"]), ("baseline", 2))
        baseline["request_countries"] = ["CAN"]
        self.assertEqual(checks.jump(SCOPE, [baseline], 2, 1), (None, None))

    def test_acknowledged_jump_is_not_republished(self):
        run = dict(SCOPE, curated_status="complete", dq_status="failed",
                   dq_jump_message_id="already-acknowledged", raw_status="complete")
        runs = MagicMock()
        runs.get.return_value = run
        sns, reporter = Mock(), Mock()
        reporter.event.return_value = {"event":"dq_completed"}
        with patch.object(quality, "database", return_value="warehouse"), \
             patch.object(quality, "clients", return_value=(Mock(), Mock(), sns)), \
             patch.object(quality, "Reporter", return_value=reporter), \
             patch.object(quality, "Runs", return_value=runs), \
             patch.object(quality.services, "initialize"), patch.object(quality.services, "set_status"), \
             patch.object(quality.services, "facts", return_value=[]), \
             patch.object(quality.services, "prior_evidence", return_value=[]), \
             patch.object(quality.services, "baselines", return_value=[]), \
             patch.object(quality.services, "replace_findings"), \
             patch.object(quality, "read_extract", return_value={"observation":{}}), \
             patch.object(quality.checks, "validate_config", return_value=CONFIG), \
             patch.object(quality.checks, "evaluate", return_value=([], {})), \
             patch.object(quality.checks, "jump", return_value=(
                 {"check_name":"observation_disappearance_jump"}, {"event":"jump"})), \
             patch("pathlib.Path.read_text", return_value="{}"), \
             patch.dict(os.environ, {"WORLD_BANK_ALERT_TOPIC_ARN":"local"}), \
             redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(quality.process(argparse.Namespace(ingestion_id="current")), 0)
        sns.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
