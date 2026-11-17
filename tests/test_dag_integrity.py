"""DAG integrity: the DAGs import, have no cycles, and are wired the way the README says."""

from airflow.sdk.definitions.asset import AssetAll

from pipeline.alerts import notify_failure
from pipeline.settings import MARTS, RAW_GREEN, RAW_YELLOW

EXPECTED_DAGS = {"tlc_ingest", "dbt_warehouse", "publish_marts"}
# Quality checks fail on facts, not glitches; retrying them would only delay the alert.
NO_RETRY_TASKS = {
    ("dbt_warehouse", "quality_gate.dbt_test_core"),
    ("dbt_warehouse", "quality_gate.check_file_quality"),
}


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags(dagbag):
    assert set(dagbag.dag_ids) == EXPECTED_DAGS


def test_no_cycles(dagbag):
    for dag in dagbag.dags.values():
        remaining = {t.task_id: set(t.upstream_task_ids) for t in dag.tasks}
        while remaining:
            ready = [tid for tid, ups in remaining.items() if not ups]
            assert ready, f"{dag.dag_id} has a cycle among {sorted(remaining)}"
            for tid in ready:
                del remaining[tid]
            for ups in remaining.values():
                ups.difference_update(ready)


def test_tags_and_docs(dagbag):
    for dag in dagbag.dags.values():
        assert dag.tags, f"{dag.dag_id} has no tags"
        assert dag.doc_md, f"{dag.dag_id} has no doc_md"


def test_retries_and_alerting(dagbag):
    for dag in dagbag.dags.values():
        assert dag.max_active_runs == 1, dag.dag_id
        for task in dag.tasks:
            key = (dag.dag_id, task.task_id)
            if key in NO_RETRY_TASKS:
                assert task.retries == 0, key
            else:
                assert task.retries >= 1, key
            assert notify_failure in _callbacks(task.on_failure_callback), key


def test_ingest_is_cron_scheduled_and_emits_raw_assets(dagbag):
    dag = dagbag.dags["tlc_ingest"]
    assert dag.schedule == "0 6 20 * *"
    assert dag.catchup is False
    assert dag.get_task("ingest_yellow").outlets == [RAW_YELLOW]
    assert dag.get_task("ingest_green").outlets == [RAW_GREEN]


def test_dbt_runs_on_both_raw_assets(dagbag):
    condition = dagbag.dags["dbt_warehouse"].timetable.asset_condition
    assert isinstance(condition, AssetAll)
    assert set(condition.objects) == {RAW_YELLOW, RAW_GREEN}


def test_aggregates_sit_behind_the_quality_gate(dagbag):
    dag = dagbag.dags["dbt_warehouse"]
    aggregates = dag.get_task("dbt_build_aggregates")
    assert aggregates.upstream_task_ids == {
        "quality_gate.dbt_test_core",
        "quality_gate.check_file_quality",
    }
    assert aggregates.trigger_rule == "all_success"
    for gate in aggregates.upstream_task_ids:
        assert dag.get_task(gate).upstream_task_ids == {"dbt_run_core"}


def test_only_the_gated_task_emits_the_marts_asset(dagbag):
    producers = [
        (dag.dag_id, task.task_id)
        for dag in dagbag.dags.values()
        for task in dag.tasks
        if MARTS in task.outlets
    ]
    assert producers == [("dbt_warehouse", "dbt_build_aggregates")]
    publish = dagbag.dags["publish_marts"].timetable.asset_condition
    assert set(publish.objects) == {MARTS}


def _callbacks(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]
