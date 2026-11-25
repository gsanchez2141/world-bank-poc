.PHONY: platform test test-unit test-spike down destroy demo missing run ingestion stage consumer curated pipeline backfill analytics dq \
	test-ingestion test-stage test-consumer test-curated test-pipeline test-backfill test-analytics test-dq \
	test-unit-ingestion test-unit-stage test-unit-consumer test-unit-curated test-unit-pipeline test-unit-backfill test-unit-analytics test-unit-dq

COMPOSE_INFRA = docker compose -f compose.infra.yaml
COMPOSE_ALL = docker compose -f compose.infra.yaml -f compose.jobs.yaml

platform:
	@mkdir -p exports
	@$(COMPOSE_INFRA) up -d --build --wait || { echo "error: health check failed" >&2; exit 1; }
	@echo "healthy"
	@$(COMPOSE_ALL) build dbt-postgres lakehouse-ingest-world-bank lakehouse-stage-world-bank lakehouse-consumer-postgres
	@if grep -E 'https://[^[:space:]"]+' infra/terraform/providers.tf | grep -v 'https://127.0.0.1:9297'; then \
		echo "error: provider endpoints are not all https://127.0.0.1:9297" >&2; \
		exit 1; \
	fi
	@./scripts/emulator-env.sh terraform -chdir=infra/terraform init -input=false
	@./scripts/emulator-env.sh terraform -chdir=infra/terraform apply -input=false -auto-approve
	@echo "apply finished"
	@./scripts/emulator-env.sh ./scripts/prove-topic.sh
	@./scripts/emulator-env.sh ./scripts/email-subscribe.sh

# Explicit selector goals limit the aggregate; without selectors, run every installed suite.
TEST_SELECTORS = $(filter ingestion stage consumer curated pipeline backfill analytics dq,$(MAKECMDGOALS))
INSTALLED_SUITES = ingestion stage consumer curated pipeline backfill analytics dq

test: $(addprefix test-,$(if $(TEST_SELECTORS),$(TEST_SELECTORS),$(INSTALLED_SUITES)))

ingestion stage consumer curated pipeline backfill analytics dq:

test-unit: test-unit-ingestion test-unit-stage test-unit-consumer test-unit-curated test-unit-pipeline test-unit-backfill test-unit-analytics test-unit-dq

test-unit-ingestion:
	@$(COMPOSE_ALL) build lakehouse-ingest-world-bank
	@docker run --rm --network none --entrypoint /bin/sh lakehouse-ingest-world-bank -c "coverage run --source=world_bank_ingest -m unittest discover -s tests -p test_unit.py && coverage report -m"

test-unit-stage:
	@$(COMPOSE_ALL) build lakehouse-stage-world-bank
	@docker run --rm --network none --entrypoint /bin/sh lakehouse-stage-world-bank -c "coverage run --source=world_bank_stage -m unittest discover -s stage_tests -p test_unit.py && coverage report -m"

test-unit-consumer:
	@$(COMPOSE_ALL) build lakehouse-consumer-postgres
	@docker run --rm --network none --entrypoint /bin/sh lakehouse-consumer-postgres -c "coverage run --source=world_bank_consumer -m unittest discover -s consumer_tests -p test_unit.py && coverage report -m"

test-ingestion: test-unit-ingestion
	@WORLD_BANK_ALERT_TOPIC_ARN=$$(./scripts/emulator-env.sh terraform -chdir=infra/terraform output -raw topic_arn) \
		$(COMPOSE_ALL) run --rm --no-deps --entrypoint python lakehouse-ingest-world-bank -m unittest discover -s tests -p test_integration.py

test-stage: test-unit-stage
	@WORLD_BANK_ALERT_TOPIC_ARN=$$(./scripts/emulator-env.sh terraform -chdir=infra/terraform output -raw topic_arn) \
		$(COMPOSE_ALL) run --rm --no-deps --entrypoint python lakehouse-stage-world-bank -m unittest discover -s stage_tests -p test_integration.py

test-consumer: test-unit-consumer
	@WORLD_BANK_ALERT_TOPIC_ARN=$$(./scripts/emulator-env.sh terraform -chdir=infra/terraform output -raw topic_arn) \
		$(COMPOSE_ALL) run --rm --no-deps --entrypoint python lakehouse-consumer-postgres -m unittest discover -s consumer_tests -p test_integration.py

test-unit-curated:
	@$(COMPOSE_ALL) build dbt-postgres
	@docker run --rm --network none --entrypoint /bin/sh dbt-postgres -c "coverage run --source=world_bank_curated -m unittest discover -s curated_tests -p test_unit.py && coverage report -m"

test-curated: test-unit-curated
	@WORLD_BANK_ALERT_TOPIC_ARN=$$(./scripts/emulator-env.sh terraform -chdir=infra/terraform output -raw topic_arn) \
		$(COMPOSE_ALL) run --rm --no-deps --entrypoint python dbt-postgres -m unittest discover -s curated_tests -p test_integration.py

test-unit-pipeline:
	@docker run --rm --network none --entrypoint python \
		-v "$(CURDIR)/dags:/opt/airflow/dags:ro" \
		-v "$(CURDIR)/config:/opt/world-bank/config:ro" \
		-v "$(CURDIR)/images/ingest-world-bank/world_bank_ingest/pipeline_run.sql:/opt/world-bank/pipeline_run.sql:ro" \
		-v "$(CURDIR)/scripts:/opt/world-bank/scripts:ro" \
		-v "$(CURDIR)/tests/pipeline:/opt/world-bank/tests:ro" \
		-e PYTHONPATH=/opt/airflow/dags apache/airflow:2.10.5 \
		-m unittest discover -s /opt/world-bank/tests -p test_unit.py

test-pipeline: test-unit-pipeline
	@python3 -m unittest discover -s tests/pipeline -p test_integration.py

test-unit-backfill:
	@docker run --rm --network none --entrypoint python \
		-v "$(CURDIR)/dags:/opt/airflow/dags:ro" \
		-v "$(CURDIR)/tests/pipeline:/opt/world-bank/tests:ro" \
		-e PYTHONPATH=/opt/airflow/dags apache/airflow:2.10.5 \
		-m unittest discover -s /opt/world-bank/tests -p test_backfill_unit.py

test-backfill: test-unit-backfill
	@python3 -m unittest discover -s tests/pipeline -p test_backfill_integration.py

test-unit-analytics:
	@$(COMPOSE_ALL) build dbt-postgres
	@docker run --rm --network none --entrypoint /bin/sh dbt-postgres -c "coverage run --source=world_bank_analytics -m unittest discover -s analytics_tests -p test_unit.py && coverage report -m"

test-analytics: test-unit-analytics
	@WORLD_BANK_ALERT_TOPIC_ARN=$$(./scripts/emulator-env.sh terraform -chdir=infra/terraform output -raw topic_arn) \
		$(COMPOSE_ALL) run --rm --no-deps --entrypoint python dbt-postgres -m unittest discover -s analytics_tests -p test_views.py
	@WORLD_BANK_ALERT_TOPIC_ARN=$$(./scripts/emulator-env.sh terraform -chdir=infra/terraform output -raw topic_arn) \
		$(COMPOSE_ALL) run --rm --no-deps --entrypoint python dbt-postgres -m unittest discover -s analytics_tests -p test_integration.py

test-unit-dq:
	@$(COMPOSE_ALL) build dbt-postgres
	@docker run --rm --network none --entrypoint /bin/sh dbt-postgres -c "coverage run --source=world_bank_dq -m unittest discover -s dq_tests -p test_unit.py && coverage report -m"

test-dq: test-unit-dq
	@python3 -m unittest discover -s tests/pipeline -p test_dq_integration.py

test-spike:
	@health=$$(curl -sf --cacert "$(HOME)/.aws/emulator/9297/ca.pem" https://127.0.0.1:9297/__admin/health || true); \
	case "$$health" in \
		ok|ok*) ;; \
		*) echo "error: emulator is not healthy; run make platform" >&2; exit 1 ;; \
	esac
	@$(COMPOSE_INFRA) exec -T postgres pg_isready -U postgres -d warehouse >/dev/null \
		|| { echo "error: warehouse is down; run make platform" >&2; exit 1; }
	@$(COMPOSE_ALL) build iceberg-spike
	@./spikes/iceberg/run.sh
	@$(COMPOSE_ALL) run --rm --entrypoint /bin/sh \
		-v "$(CURDIR)/spikes/dbt/run.sh:/opt/spike/run.sh:ro" \
		dbt-postgres /opt/spike/run.sh

down:
	@$(COMPOSE_INFRA) down

destroy:
	@$(COMPOSE_INFRA) down --volumes --remove-orphans

run:
	@python3 scripts/run-pipeline.py

ifneq ($(filter missing,$(MAKECMDGOALS)),)
demo:
	@$(COMPOSE_ALL) build aws-emulator iceberg-rest lakehouse-ingest-world-bank lakehouse-stage-world-bank lakehouse-consumer-postgres dbt-postgres
	@python3 scripts/demo-missing.py

missing:
	@:
else
demo: platform
	@python3 scripts/run-pipeline.py --demo
endif
