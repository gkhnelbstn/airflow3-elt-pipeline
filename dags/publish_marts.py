"""Record a publication of the aggregate marts, after the quality gate passed.

Scheduled on the `marts.aggregates` asset, which only `dbt_build_aggregates`
emits. A run blocked by the quality gate emits nothing, so this DAG does not run
and `ops.publish_log` gets no row: the stand-in for "dashboards refreshed".
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from pipeline.settings import DEFAULT_ARGS, MARTS, WAREHOUSE_CONN_ID

DDL = [
    "create schema if not exists ops",
    """
create table if not exists ops.publish_log (
    published_at  timestamptz not null default now(),
    run_id        text        not null,
    model         text        not null,
    row_count     bigint      not null,
    months        text        not null
    )""",
]
PUBLISH_SQL = """
insert into ops.publish_log (run_id, model, row_count, months)
select %(run_id)s, 'agg_trips_monthly', count(*),
       coalesce(string_agg(distinct to_char(month_start, 'YYYY-MM'), ','), '')
from marts.agg_trips_monthly
union all
select %(run_id)s, 'agg_trips_daily', count(*),
       coalesce(string_agg(distinct to_char(source_month, 'YYYY-MM'), ','), '')
from marts.agg_trips_daily;
"""


@dag(
    dag_id="publish_marts",
    description="Log a publication of the aggregate marts (runs only behind a passed gate)",
    schedule=[MARTS],
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["publish", "warehouse"],
    doc_md=__doc__,
)
def publish_marts():
    @task
    def record_publication(**context) -> None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        hook.run(DDL)
        hook.run(PUBLISH_SQL, parameters={"run_id": context["run_id"]})

    record_publication()


publish_marts()
