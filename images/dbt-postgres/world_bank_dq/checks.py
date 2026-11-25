"""Pure curated quality checks and scoped disappearance detection."""
from decimal import Decimal, InvalidOperation, localcontext
import re

POPULATION = "SP.POP.TOTL"
GDP = "NY.GDP.MKTP.CD"
PER_CAPITA = "NY.GDP.PCAP.CD"


def validate_config(mapping):
    try:
        config = mapping["data_quality"]
        tolerances = config["gdp_per_capita"]
        values = []
        for item in (tolerances["absolute_usd"], tolerances["relative_fraction"]):
            if type(item) is int:
                value = Decimal(item)
            elif isinstance(item, str) and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", item):
                value = Decimal(item)
            else:
                raise ValueError("invalid tolerance")
            if not value.is_finite() or value < 0:
                raise ValueError("invalid tolerance")
            values.append(value)
        jump = config["disappearance_jump_threshold"]
        if type(jump) is not int or jump < 0:
            raise ValueError("invalid jump threshold")
        return {"absolute_usd": values[0], "relative_fraction": values[1], "disappearance_jump_threshold": jump}
    except (KeyError, TypeError, InvalidOperation) as error:
        raise ValueError("invalid data quality configuration") from error


def _number(value):
    if value is None:
        return None
    if isinstance(value, float) or isinstance(value, bool):
        raise ValueError("invalid numeric value")
    try:
        number = Decimal(value)
    except (TypeError, ValueError, InvalidOperation) as error:
        raise ValueError("invalid numeric value") from error
    if not number.is_finite():
        raise ValueError("nonfinite numeric value")
    return number


def _key(row):
    return (row["country_iso3"], row["indicator_code"], int(row["year"]))


def _finding(name, severity, key, message):
    return {"check_name": name, "severity": severity, "business_key": dict(country_iso3=key[0], indicator_code=key[1], year=key[2]), "message": message}


def evaluate(run, facts, source_records, prior_evidence, config):
    findings = []
    diagnostics = {"gdp_checked": 0, "gdp_skipped": 0}
    indexed = {}
    for fact in facts:
        key = _key(fact)
        if key in indexed:
            raise ValueError("duplicate curated business key")
        value = _number(fact["value"])
        indexed[key] = (fact, value)
        if key[1] == POPULATION and value is not None and value <= 0:
            findings.append(_finding("population_positive", "error", key, f"Population must be positive; received {value}."))
    source = source_records or {}
    for country, year in sorted({(k[0], k[2]) for k in indexed if k[1] in (GDP, POPULATION, PER_CAPITA)}):
        keys = [(country, code, year) for code in (GDP, POPULATION, PER_CAPITA)]
        trio = [indexed.get(key) for key in keys]
        if any(item is None or item[1] is None for item in trio) or trio[1][1] <= 0:
            diagnostics["gdp_skipped"] += 1
            continue
        raw_ids = [item[0]["ingestion_id"] for item in trio]
        common = all(raw_ids) and len(set(raw_ids)) == 1
        if not common and run["mode"] == "extract":
            common = all(key in source and _number(source[key]["value"]) == item[1] for key, item in zip(keys, trio))
        if not common:
            diagnostics["gdp_skipped"] += 1
            continue
        diagnostics["gdp_checked"] += 1
        with localcontext() as context:
            context.prec = 100
            calculated = trio[0][1] / trio[1][1]
            reported = trio[2][1]
            tolerance = max(config["absolute_usd"], config["relative_fraction"] * abs(calculated))
            if abs(reported - calculated) > tolerance:
                findings.append(_finding("gdp_per_capita_consistency", "error", keys[2], f"GDP per capita {reported} differs from GDP/population {calculated} by more than {tolerance}."))
    if run["mode"] == "extract":
        current_order = (run["raw_ingested_at"], run["ingestion_id"])
        evidence = list(facts) + list(prior_evidence)
        eligible = set()
        for row in evidence:
            key = _key(row)
            if key not in indexed or key[0] not in run["request_countries"] or key[1] not in run["request_indicators"] or not run["start_year"] <= key[2] <= run["end_year"]:
                continue
            source_id = row["ingestion_id"]
            source_time = row.get("source_raw_ingested_at") or row.get("raw_ingested_at")
            if source_id and source_id != run["ingestion_id"] and source_time is not None and (source_time, source_id) < current_order:
                eligible.add(key)
        for key in sorted(eligible - set(source)):
            findings.append(_finding("observation_disappeared", "warning", key, "Previously received observation was absent from this extract."))
    return findings, diagnostics


def jump(run, baselines, count, threshold):
    if run["mode"] != "extract":
        return None, None
    current = (run["raw_ingested_at"], run["ingestion_id"])
    comparable = [row for row in baselines if row["mode"] == "extract" and row["raw_status"] == "complete" and row["dq_status"] == "complete" and (row["raw_ingested_at"], row["ingestion_id"]) < current and set(row["request_countries"]) == set(run["request_countries"]) and set(row["request_indicators"]) == set(run["request_indicators"]) and row["start_year"] == run["start_year"] and row["end_year"] == run["end_year"]]
    if not comparable:
        return None, None
    baseline = max(comparable, key=lambda row: (row["raw_ingested_at"], row["ingestion_id"]))
    prior = baseline["warning_count"]
    increase = count - prior
    if increase <= threshold:
        return None, None
    payload = dict(ingestion_id=run["ingestion_id"], task_name="data_quality", event="observation_disappearance_jump", baseline_ingestion_id=baseline["ingestion_id"], prior_count=prior, current_count=count, increase=increase, threshold=threshold)
    message = f"Baseline {baseline['ingestion_id']}: {prior} warnings; current {count}; increase {increase}; threshold {threshold}."
    return dict(check_name="observation_disappearance_jump", severity="warning", business_key={}, message=message), payload
