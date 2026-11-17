# Airflow 3 plus the warehouse it orchestrates.
#
# The warehouse (the `tlc-ingest` loader and the dbt project) comes from
# https://github.com/gkhnelbstn/dbt-postgres-warehouse-demo at a pinned release and
# is installed into its own virtualenv, so dbt's dependencies never meet Airflow's.
# DAGs call it as /opt/warehouse-venv/bin/{tlc-ingest,dbt}.
FROM apache/airflow:3.4.0

ARG WAREHOUSE_REPO=gkhnelbstn/dbt-postgres-warehouse-demo
# A release tag of the warehouse repo (v1.0.0 is commit 6c1e305). Any tag, branch
# or commit SHA works: docker compose build --build-arg WAREHOUSE_REF=<ref>
ARG WAREHOUSE_REF=v1.0.0

USER root
# Group 0 and group-writable, like the base image, so any AIRFLOW_UID can write.
# New named volumes mounted on the last two inherit this ownership.
RUN mkdir -p /opt/warehouse /opt/warehouse-venv /opt/airflow/tlc-cache /opt/airflow/dbt-target \
    && chown -R airflow:0 /opt/warehouse /opt/warehouse-venv /opt/airflow/tlc-cache /opt/airflow/dbt-target \
    && chmod -R g+rwX /opt/warehouse /opt/airflow/tlc-cache /opt/airflow/dbt-target
USER airflow

# Source at that ref (the repo is public; no git needed in the image).
RUN python - <<EOF
import io, tarfile, urllib.request
url = "https://codeload.github.com/${WAREHOUSE_REPO}/tar.gz/${WAREHOUSE_REF}"
with urllib.request.urlopen(url) as r, tarfile.open(fileobj=io.BytesIO(r.read())) as tar:
    for m in tar.getmembers():
        parts = m.name.split("/", 1)
        if len(parts) == 2 and parts[1]:
            m.name = parts[1]
            tar.extract(m, "/opt/warehouse", filter="data")
EOF

# Versions match the warehouse repo's uv.lock at that commit.
RUN python -m venv /opt/warehouse-venv \
    && /opt/warehouse-venv/bin/pip install --no-cache-dir --quiet \
        /opt/warehouse "dbt-core==1.12.7" "dbt-postgres==1.11.0" \
    && cd /opt/warehouse/dbt \
    && DBT_PROFILES_DIR=. /opt/warehouse-venv/bin/dbt deps --quiet

ENV WAREHOUSE_HOME=/opt/warehouse \
    DBT_PROJECT_DIR=/opt/warehouse/dbt \
    DBT_PROFILES_DIR=/opt/warehouse/dbt \
    DBT_TARGET_PATH=/opt/airflow/dbt-target \
    DBT_LOG_PATH=/opt/airflow/logs/dbt \
    TLC_CACHE_DIR=/opt/airflow/tlc-cache
