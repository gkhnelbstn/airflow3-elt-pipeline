"""Monthly load of NYC TLC yellow and green trip files into the warehouse.

Each run loads one TLC month, two months before the run's month (TLC publishes
with about that delay), using the warehouse repo's `tlc-ingest` CLI. The loader
is idempotent: a file already loaded with the same published version is skipped,
so retries, re-runs and overlapping backfills are safe.

Each successful task emits an event on its raw-table asset, with the month, the
outcome and the row counts in `extra`. `dbt_warehouse` is scheduled on both.

Backfill a range of months (the logical dates' months minus two are loaded):

    airflow backfill create --dag-id tlc_ingest \
        --from-date 2026-09-20 --to-date 2026-11-20
"""

from __future__ import annotations

import logging
import os
import subprocess

import pendulum
from airflow.sdk import Asset, Param, dag, task

from pipeline.settings import (
    DEFAULT_ARGS,
    RAW_GREEN,
    RAW_YELLOW,
    TLC_INGEST,
    WAREHOUSE_CONN_ID,
    max_rows,
    target_month,
)

log = logging.getLogger(__name__)


def resolve_month(context) -> str:
    """YYYY-MM to load: the `month` param if given, else derived from the run date."""
    if context["params"].get("month"):
        return context["params"]["month"]
    run_date = context.get("logical_date") or context["dag_run"].run_after
    return f"{target_month(run_date):%Y-%m}"


def ingest_command(dataset: str, month: str, base_url: str, rows: int) -> list[str]:
    cmd = [TLC_INGEST, "--dataset", dataset, "--from", month, "--to", month]
    cmd += ["--base-url", base_url]
    if rows > 0:
        cmd += ["--max-rows", str(rows)]
    return cmd


def run_ingest(dataset: str, asset: Asset, context) -> dict:
    month = resolve_month(context)
    base_url = context["params"].get("base_url") or os.environ["TLC_BASE_URL"]
    cmd = ingest_command(dataset, month, base_url, max_rows())
    log.info("running: %s", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    output = []
    for line in proc.stdout:
        output.append(line.rstrip())
        log.info("%s", line.rstrip())
    if proc.wait() != 0:
        raise RuntimeError(f"tlc-ingest exited with {proc.returncode} for {dataset} {month}")

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    row = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_first(
        """
        select source_rows, loaded_rows, source_version, finished_at
        from raw.load_log
        where dataset = %s and period = %s::date and status = 'succeeded'
        order by load_id desc limit 1
        """,
        parameters=(dataset, f"{month}-01"),
    )
    if row is None:
        raise RuntimeError(f"no successful load of {dataset} {month} in raw.load_log")
    loaded_now = any(f"{dataset} {month}: loaded" in line for line in output)
    extra = {
        "dataset": dataset,
        "month": month,
        "outcome": "loaded" if loaded_now else "already_loaded",
        "file_rows": row[0],
        "raw_rows": row[1],
        "file_version": row[2],
        "base_url": base_url,
    }
    context["outlet_events"][asset].extra = extra
    return extra


@dag(
    dag_id="tlc_ingest",
    description="Load one TLC month (yellow + green) into raw.* and emit raw-table assets",
    schedule="0 6 20 * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tlc", "ingest", "warehouse"],
    params={
        "month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="YYYY-MM to load instead of the month derived from the run date",
        ),
        "base_url": Param(
            None,
            type=["null", "string"],
            description="Download from here instead of TLC_BASE_URL (e.g. http://fake-cdn)",
        ),
    },
    doc_md=__doc__,
)
def tlc_ingest():
    # Sequential: both calls also refresh the shared zone lookup table.
    @task(outlets=[RAW_YELLOW])
    def ingest_yellow(**context) -> dict:
        return run_ingest("yellow", RAW_YELLOW, context)

    @task(outlets=[RAW_GREEN])
    def ingest_green(**context) -> dict:
        return run_ingest("green", RAW_GREEN, context)

    ingest_yellow() >> ingest_green()


tlc_ingest()
