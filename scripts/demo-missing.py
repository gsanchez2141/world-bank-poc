"""Run a private fixture to demonstrate observations omitted by a later extract."""
import csv
from decimal import Decimal
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests" / "pipeline"))
from fixture import PrivatePipeline

INDICATORS = ["SP.POP.TOTL", "NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD"]
EXPECTED = {"NY.GDP.MKTP.CD": Decimal("1000"), "NY.GDP.PCAP.CD": Decimal("10")}
FIELDS = ["country_iso3", "indicator_code", "year", "value", "reason"]


def run(fixture, mode):
    (fixture.home / "control").write_text(mode)
    conf = {
        "ingestion_id": mode,
        "countries": ["BRA"],
        "indicators": INDICATORS,
        "start_year": 2021,
        "end_year": 2021,
    }
    run_id = "manual_" + mode
    fixture.trigger("world_bank_pipeline", run_id, conf)
    state = fixture.wait("world_bank_pipeline", run_id)
    print(f"{run_id}: {state}", flush=True)
    if state != "success":
        raise RuntimeError(f"Pipeline run {run_id} failed")


def main():
    with PrivatePipeline() as fixture:
        run(fixture, "dq_seed")
        run(fixture, "dq_omit")
        source = fixture.home / "exports" / "vw_missing_or_incomplete.csv"
        with source.open(newline="") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != FIELDS:
                raise ValueError("Unexpected missing-observation report columns")
            rows = [
                row for row in reader
                if row["country_iso3"] == "BRA" and row["year"] == "2021"
                and row["indicator_code"] in EXPECTED
            ]
        if len(rows) != 2 or {row["indicator_code"] for row in rows} != set(EXPECTED):
            raise ValueError("Missing expected Brazil observations")
        for row in rows:
            if row["reason"] != "MISSING_FROM_LATEST_EXTRACT" or Decimal(row["value"]) != EXPECTED[row["indicator_code"]]:
                raise ValueError("Omission report does not contain the expected retained value")

        output = ROOT / "exports" / "vw_missing_or_incomplete_demo.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", newline="", dir=output.parent, delete=False) as stream:
                temporary = Path(stream.name)
                writer = csv.DictWriter(stream, fieldnames=FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            os.replace(temporary, output)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
        print(f"Demonstration report: {output}")
        for row in rows:
            print(", ".join(row[field] for field in FIELDS))


if __name__ == "__main__":
    main()
