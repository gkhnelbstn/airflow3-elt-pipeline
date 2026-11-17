#!/usr/bin/env bash
# End-to-end check against the running compose stack (used by CI; works locally too).
#
#   1. A good month: tlc_ingest -> dbt_warehouse (gate passes) -> publish_marts.
#   2. A corrupted month from the fake CDN: dbt_warehouse fails at the quality gate,
#      the aggregates are upstream_failed, and publish_marts does not run again.
#
# Usage: scripts/e2e.sh GOOD_MONTH BAD_MONTH   (both YYYY-MM, published by TLC)
set -euo pipefail
GOOD=${1:?good month YYYY-MM}
BAD=${2:?bad month YYYY-MM}
TIMEOUT=${E2E_TIMEOUT:-900}

af() { docker compose exec -T airflow-scheduler airflow "$@" 2>/dev/null; }

runs() {  # runs <dag_id> -> JSON list of runs
    af dags list-runs "$1" -o json
}

wait_runs() {  # wait_runs <dag_id> <n>: wait until n runs exist and none is active
    local dag=$1 n=$2 waited=0
    while true; do
        local json; json=$(runs "$dag" || echo '[]')
        local total active
        total=$(jq length <<<"$json")
        active=$(jq '[.[] | select(.state == "queued" or .state == "running")] | length' <<<"$json")
        if (( total >= n && active == 0 )); then echo "$json"; return 0; fi
        if (( waited >= TIMEOUT )); then
            echo "timed out waiting for $dag ($total runs, $active active)" >&2; return 1
        fi
        sleep 10; waited=$((waited + 10))
    done
}

latest_state() { jq -r 'sort_by(.start_date) | last | .state' <<<"$1"; }
latest_run_id() { jq -r 'sort_by(.start_date) | last | .run_id' <<<"$1"; }

for dag in publish_marts dbt_warehouse tlc_ingest; do af dags unpause "$dag" >/dev/null; done
# Unpausing tlc_ingest creates its latest scheduled run, which may trigger the
# others; let that settle so the run counts below are deterministic.
wait_runs tlc_ingest 1 >/dev/null
sleep 20
dbt_base=$(wait_runs dbt_warehouse 0 | jq length)
sleep 20
pub_base=$(wait_runs publish_marts 0 | jq length)
base=$(runs tlc_ingest | jq length)

echo "== good month $GOOD"
af dags trigger tlc_ingest --conf "{\"month\": \"$GOOD\"}" >/dev/null
[[ $(latest_state "$(wait_runs tlc_ingest $((base + 1)))") == success ]]
dbt=$(wait_runs dbt_warehouse $((dbt_base + 1)))
[[ $(latest_state "$dbt") == success ]] || { echo "dbt_warehouse did not succeed" >&2; exit 1; }
pub=$(wait_runs publish_marts $((pub_base + 1)))
[[ $(latest_state "$pub") == success ]] || { echo "publish_marts did not succeed" >&2; exit 1; }
pub_count=$(jq length <<<"$pub")
echo "green path ok"

echo "== corrupted month $BAD"
docker compose run --rm -T airflow-cli bash -c \
    "/opt/warehouse-venv/bin/python /opt/airflow/scripts/make_bad_month.py --month $BAD"
docker compose --profile faults up -d fake-cdn
af dags trigger tlc_ingest --conf "{\"month\": \"$BAD\", \"base_url\": \"http://fake-cdn\"}" >/dev/null
[[ $(latest_state "$(wait_runs tlc_ingest $((base + 2)))") == success ]]
dbt=$(wait_runs dbt_warehouse $((dbt_base + 2)))
run_id=$(latest_run_id "$dbt")
[[ $(latest_state "$dbt") == failed ]] || { echo "the gate did not block $BAD" >&2; exit 1; }
states=$(af tasks states-for-dag-run dbt_warehouse "$run_id" -o json)
echo "$states" | jq -r '.[] | "\(.task_id): \(.state)"'
check() { [[ $(jq -r --arg t "$1" '.[] | select(.task_id == $t) | .state' <<<"$states") == "$2" ]]; }
check dbt_run_core success
check quality_gate.dbt_test_core failed
check quality_gate.check_file_quality failed
check dbt_build_aggregates upstream_failed
sleep 30  # give the scheduler time to (wrongly) start publish_marts
[[ $(runs publish_marts | jq length) == "$pub_count" ]] || { echo "publish_marts ran" >&2; exit 1; }
echo "blocked path ok"
