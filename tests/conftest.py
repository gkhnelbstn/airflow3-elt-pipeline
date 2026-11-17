import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
# Airflow puts the DAGs folder on sys.path; do the same so `pipeline` imports.
sys.path.insert(0, str(DAGS_DIR))


@pytest.fixture(scope="session")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS_DIR))
