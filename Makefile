.PHONY: platform test test-unit test-spike down demo run ingestion stage consumer curated \
	test-ingestion test-stage test-consumer test-curated \
	test-unit-ingestion test-unit-stage test-unit-consumer test-unit-curated

COMPOSE_INFRA = docker compose -f compose.infra.yaml
COMPOSE_ALL = docker compose -f compose.infra.yaml -f compose.jobs.yaml

platform:
	@$(COMPOSE_INFRA) up -d --build --wait || { echo "error: health check failed" >&2; exit 1; }
	@echo "healthy"
	@$(COMPOSE_ALL) build dbt-postgres
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
TEST_SELECTORS = $(filter ingestion stage consumer curated,$(MAKECMDGOALS))
INSTALLED_SUITES = ingestion stage consumer curated

test: $(addprefix test-,$(if $(TEST_SELECTORS),$(TEST_SELECTORS),$(INSTALLED_SUITES)))

ingestion stage consumer curated:

test-unit: test-unit-ingestion test-unit-stage test-unit-consumer test-unit-curated

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

demo run:
	@echo "pipeline DAG is not in this MR" >&2
	@exit 1
