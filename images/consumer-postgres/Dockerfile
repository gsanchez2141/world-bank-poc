FROM python:3.12-slim
ARG DUCKDB_VERSION=2.0.1
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tar gzip \
    && rm -rf /var/lib/apt/lists/* \
    && case "$(uname -m)" in \
         aarch64|arm64) arch=arm64 ;; \
         x86_64|amd64) arch=amd64 ;; \
         *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/duckdb.tar.gz \
         "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb-cli-linux-${arch}.tar.gz" \
    && tar -xzf /tmp/duckdb.tar.gz -C /usr/local/bin \
    && rm /tmp/duckdb.tar.gz \
    && duckdb -c "INSTALL iceberg; INSTALL httpfs; SELECT version();" \
    && duckdb -csv -c "SELECT version();" | grep -q "${DUCKDB_VERSION}"
WORKDIR /app
COPY images/ingest-world-bank/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY images/ingest-world-bank/world_bank_ingest ./world_bank_ingest
COPY images/stage-world-bank/world_bank_stage ./world_bank_stage
COPY images/consumer-postgres/world_bank_consumer ./world_bank_consumer
COPY images/consumer-postgres/tests ./consumer_tests
ENTRYPOINT ["python", "-m", "world_bank_consumer"]
