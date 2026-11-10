# airflow3-elt-pipeline

Airflow 3 ELT pipeline: public API to PostgreSQL, then dbt, with a data quality gate.

**Status: planned.** This repo is a work in progress; the checklist lives in the epic issue and on the [Portfolio build-out](https://github.com/users/gkhnelbstn/projects/6) board.

## Plan

- What: Airflow 3 DAGs that pull from a public API (e.g. open weather or exchange rates) into PostgreSQL, then trigger the dbt project from #1, with a data quality gate that fails the run.
- Must have: assets/dataset-driven scheduling, retries and alerting callback, backfill example, Docker Compose, unit tests for DAG integrity.
- Done when: README has a screenshot of a green DAG run and of a run blocked by the quality gate.
