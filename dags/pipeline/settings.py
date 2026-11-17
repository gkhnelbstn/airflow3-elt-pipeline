"""Shared settings and assets for the DAGs. Not a DAG file (see .airflowignore)."""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

from airflow.sdk import Asset

from pipeline.alerts import notify_failure

WAREHOUSE_BIN = os.environ.get("WAREHOUSE_BIN", "/opt/warehouse-venv/bin")
TLC_INGEST = f"{WAREHOUSE_BIN}/tlc-ingest"
DBT = f"{WAREHOUSE_BIN}/dbt"
WAREHOUSE_CONN_ID = "warehouse"

# TLC publishes a month's files about two months after it ends.
PUBLICATION_LAG_MONTHS = 2

# One asset per raw table. The month a run loaded travels in the event's `extra`;
# assets are fixed when the DAG is parsed, so they cannot be per month.
RAW_YELLOW = Asset(
    name="raw.yellow_trips", uri="postgres://postgres:5432/warehouse/raw/yellow_trips"
)
RAW_GREEN = Asset(name="raw.green_trips", uri="postgres://postgres:5432/warehouse/raw/green_trips")
# The aggregate marts: only updated when the quality gate passed.
MARTS = Asset(name="marts.aggregates", uri="postgres://postgres:5432/warehouse/marts/aggregates")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
}


def target_month(run_date: datetime | date, lag: int = PUBLICATION_LAG_MONTHS) -> date:
    """The TLC month a run on `run_date` should load: `lag` months before its month."""
    index = run_date.year * 12 + (run_date.month - 1) - lag
    return date(index // 12, index % 12 + 1, 1)


def max_rows() -> int:
    """Rows per file to load; 0 means whole files."""
    return int(os.environ.get("TLC_MAX_ROWS", "50000"))


def min_valid_pct() -> float:
    return float(os.environ.get("QUALITY_MIN_VALID_PCT", "90"))
