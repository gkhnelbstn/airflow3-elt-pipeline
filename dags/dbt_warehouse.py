"""Build the warehouse with dbt, behind a data quality gate.

Scheduled on the raw-table assets: runs when both `raw.yellow_trips` and
`raw.green_trips` have new events (one `tlc_ingest` run emits both). Several
queued events collapse into one run, which is fine: dbt's incremental models
read from `raw.load_log` which months changed, not from the events.

    dbt_run_core ──> quality_gate ─┬─ dbt_test_core       (all dbt tests on core models)
                                   └─ check_file_quality  (mart_file_quality, per file)
                 ──> dbt_build_aggregates  ──> asset marts.aggregates ──> publish_marts

If any gate check fails, the aggregates are not built (`upstream_failed`), no
`marts.aggregates` event is emitted, and `publish_marts` does not run.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task, task_group

from pipeline.settings import (
    DBT,
    DEFAULT_ARGS,
    MARTS,
    RAW_GREEN,
    RAW_YELLOW,
    WAREHOUSE_CONN_ID,
    min_valid_pct,
)

log = logging.getLogger(__name__)

AGGREGATES = "path:models/marts/aggregates"
QUALITY_SQL = """
select service_type, to_char(source_month, 'YYYY-MM'), file_rows, raw_rows, fct_rows,
       is_reconciled, valid_pct, unknown_zone_rows, non_positive_duration_rows
from marts.mart_file_quality
order by source_month, service_type
"""


def failed_files(rows: list[tuple], threshold: float) -> list[str]:
    """Human-readable reasons for every file that fails the gate."""
    problems = []
    for service, month, _file, raw, fct, reconciled, valid_pct, *_ in rows:
        if not reconciled:
            problems.append(f"{service} {month}: raw has {raw} rows but fct_trips has {fct}")
        if valid_pct is None or float(valid_pct) < threshold:
            problems.append(f"{service} {month}: valid_pct {valid_pct} < {threshold}")
    return problems


@dag(
    dag_id="dbt_warehouse",
    description="dbt build of the TLC warehouse with a quality gate before the aggregates",
    schedule=RAW_YELLOW & RAW_GREEN,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "warehouse", "quality"],
    doc_md=__doc__,
)
def dbt_warehouse():
    run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command=f"{DBT} seed && {DBT} run --exclude {AGGREGATES}",
    )

    # A failed check is a finding about the data, not a glitch: no retries.
    @task_group(group_id="quality_gate")
    def quality_gate():
        BashOperator(
            task_id="dbt_test_core",
            bash_command=f"{DBT} test --exclude {AGGREGATES}",
            retries=0,
        )

        @task(retries=0)
        def check_file_quality() -> None:
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            rows = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_records(QUALITY_SQL)
            log.info(
                "file | file_rows | raw_rows | fct_rows | is_reconciled | valid_pct"
                " | unknown_zone_rows (264/265) | non_positive_duration_rows"
            )
            for r in rows:
                log.info("%s %s | %s | %s | %s | %s | %s | %s | %s", *r)
            threshold = min_valid_pct()
            if problems := failed_files(rows, threshold):
                raise ValueError("quality gate failed:\n  " + "\n  ".join(problems))
            log.info("quality gate passed for %d files (valid_pct >= %s)", len(rows), threshold)

        check_file_quality()

    build_aggregates = BashOperator(
        task_id="dbt_build_aggregates",
        bash_command=f"{DBT} build --select {AGGREGATES}",
        outlets=[MARTS],
    )

    run_core >> quality_gate() >> build_aggregates


dbt_warehouse()
