# DuckDB CLI 2.0.1. Exact version. Not installed on the host.
FROM debian:bookworm-slim

ARG DUCKDB_VERSION=2.0.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl python3 tar gzip \
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

WORKDIR /opt/spike
