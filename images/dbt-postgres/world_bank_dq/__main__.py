"""Run curated quality checks and persist scoped findings."""
import argparse
import json
import os
from pathlib import Path
import re
import sys

import yaml

from world_bank_curated.services import Reporter, Runs, clients, database
from world_bank_stage.input import read_extract
from . import checks, services


def process(args):
    reporter, runs = None, None
    accepted = False
    try:
        database_name = database()
        s3, logs, sns = clients()
        topic = os.environ["WORLD_BANK_ALERT_TOPIC_ARN"]
        reporter = Reporter(logs, sns, args.ingestion_id, topic)
        reporter.open()
        reporter.event("INFO", "dq_started")
        runs = Runs(args.ingestion_id, database_name)
        run = runs.get(args.ingestion_id)
        if not run:
            raise ValueError("quality run not found")
        accepted = True
        if run.get("dq_status") == "complete":
            print(json.dumps(reporter.event("INFO", "dq_already_complete")))
            return 0
        if run["curated_status"] != "complete":
            raise ValueError("curated run is not complete")
        services.initialize(runs)
        run = runs.get(args.ingestion_id)
        config_path = "/usr/app/config/pipeline.yml"
        if database_name.startswith("curated_test_"):
            config_path = os.environ.get("WORLD_BANK_DQ_TEST_CONFIG", config_path)
        config = checks.validate_config(yaml.safe_load(Path(config_path).read_text()))
        services.set_status(runs, "pending")
        with runs.db.transaction():
            runs.db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            run = runs.get(args.ingestion_id)
            if run["mode"] == "extract":
                if run["raw_status"] != "complete":
                    raise ValueError("extract raw data is incomplete")
                source = read_extract(s3, run)["observation"]
            else:
                source = {}
            findings, diagnostics = checks.evaluate(run, services.facts(runs.db), source,
                                                     services.prior_evidence(runs.db), config)
            hard_failure = any(item["severity"] == "error" for item in findings)
            warning_count = sum(item["check_name"] == "observation_disappeared" for item in findings)
            jump_finding, jump_payload = (None, None)
            if not hard_failure:
                jump_finding, jump_payload = checks.jump(
                    run, services.baselines(runs.db), warning_count, config["disappearance_jump_threshold"])
                if jump_finding:
                    findings.append(jump_finding)
            services.replace_findings(runs.db, args.ingestion_id, findings)
        runs.db.commit()
        reporter.event("INFO", "dq_checked", **diagnostics)
        if hard_failure:
            raise ValueError("curated quality checks failed")
        message_id = run.get("dq_jump_message_id")
        if jump_payload and not message_id:
            response = sns.publish(TopicArn=topic, Message=json.dumps(jump_payload))
            message_id = response["MessageId"]
        runs.db.execute("""UPDATE meta.pipeline_run SET dq_status='complete', dq_jump_message_id=%s
                           WHERE ingestion_id=%s AND dq_status != 'complete'""", (message_id, args.ingestion_id))
        runs.db.commit()
        if jump_payload and message_id:
            reporter.event("INFO", "dq_disappearance_jump_published", message_id=message_id)
        print(json.dumps(reporter.event("INFO", "dq_completed")))
        return 0
    except Exception as error:
        if runs and accepted:
            try:
                runs.db.rollback()
                services.set_status(runs, "failed")
            except Exception:
                pass
        safe_error = f"{type(error).__name__}: data quality operation failed"
        if reporter:
            reporter.failure(safe_error, event="dq_failed")
        else:
            print(json.dumps(dict(level="ERROR", event="dq_failed", ingestion_id=args.ingestion_id,
                                  error=safe_error)), file=sys.stderr)
        return 1
    finally:
        if runs:
            runs.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingestion-id", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", args.ingestion_id, re.ASCII):
        parser.error("invalid ingestion ID")
    return process(args)


if __name__ == "__main__":
    raise SystemExit(main())
