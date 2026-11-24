# dbt 2.0.12 official Linux binary. Do not replace this pin with dbt Core 1.x.
FROM debian:bookworm-slim

ARG DBT_VERSION=2.0.12

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libpq5 tar gzip \
    && rm -rf /var/lib/apt/lists/* \
    && case "$(uname -m)" in \
         aarch64|arm64) arch=aarch64 ;; \
         x86_64|amd64) arch=x86_64 ;; \
         *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;; \
       esac \
    && mkdir -p /tmp/dbt-extract \
    && curl -fsSL -o /tmp/dbt.tar.gz \
         "https://public.cdn.getdbt.com/fs/cli/fs-v${DBT_VERSION}-${arch}-unknown-linux-gnu.tar.gz" \
    && tar -xzf /tmp/dbt.tar.gz -C /tmp/dbt-extract \
    && install "$(find /tmp/dbt-extract -type f -name dbt | head -n 1)" /usr/local/bin/dbt \
    && rm -rf /tmp/dbt-extract /tmp/dbt.tar.gz \
    && dbt --version | grep -q "${DBT_VERSION}"

ENV DBT_ALLOW_EXPERIMENTAL_ADAPTERS=1 \
    DBT_PROFILES_DIR=/usr/app \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DO_NOT_TRACK=1

WORKDIR /usr/app
COPY dbt_project.yml profiles.yml ./
COPY models ./models
