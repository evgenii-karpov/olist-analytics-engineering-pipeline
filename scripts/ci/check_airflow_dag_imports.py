"""Fail CI if Airflow cannot import project DAGs."""

from __future__ import annotations

import json
import os
from pathlib import Path

# Keep the import-only CI check away from repository-mounted Airflow state.
os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow")
os.environ.setdefault("AIRFLOW__LOGGING__BASE_LOG_FOLDER", "/tmp/airflow/logs")

from airflow.dag_processing.dagbag import DagBag

PROJECT_ROOT = Path(os.environ.get("OLIST_PROJECT_ROOT", Path.cwd()))


def airflow_dags_folder() -> Path:
    configured_folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER")
    if configured_folder:
        return Path(configured_folder)
    return Path("/opt/airflow/dags")


def main() -> None:
    dags_dir = airflow_dags_folder()
    dag_bag = DagBag(dag_folder=str(dags_dir), include_examples=False)
    if dag_bag.import_errors:
        print(json.dumps(dag_bag.import_errors, indent=2, sort_keys=True))
        raise SystemExit(1)

    print(f"Imported {len(dag_bag.dags)} DAGs from {dags_dir}")


if __name__ == "__main__":
    main()
