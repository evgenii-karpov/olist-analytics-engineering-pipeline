"""Local Airflow DAG for the Olist Modern Data Stack project.

This DAG keeps the same logical pipeline as the AWS version, but uses a local
S3-shaped raw zone and PostgreSQL in Docker instead of S3 and Redshift.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG  # pyright: ignore[reportMissingImports]
from airflow.operators.bash import BashOperator  # pyright: ignore[reportMissingImports]
from airflow.operators.empty import EmptyOperator  # pyright: ignore[reportMissingImports]


DAG_ID = "olist_modern_data_stack_local"
PROJECT_ROOT = Path(
    os.environ.get("OLIST_PROJECT_ROOT", Path(__file__).resolve().parents[2])
)
PYTHON_BIN = os.environ.get("OLIST_PYTHON_BIN", "python")
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "olist_analytics"
LOCAL_RAW_DIR = "data/raw/olist"
POSTGRES_SQL_DIR = "infra/postgres"
LOCAL_RUN_ID = "{{ run_id | replace(':', '_') | replace('+', '_') }}"
LOCAL_BATCH_ID = "{{ params.batch_date }}"


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    description="Olist batch pipeline: local raw files, PostgreSQL load, and dbt transformations.",
    default_args=default_args,
    start_date=datetime(2016, 9, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["olist", "local", "postgres", "dbt"],
    params={
        "batch_date": "2018-09-01",
        "lookback_days": 3,
        "full_refresh": False,
    },
) as dag:
    start = EmptyOperator(task_id="start")

    validate_source_contract = BashOperator(
        task_id="validate_source_contract",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            f"{PYTHON_BIN} scripts/utilities/validate_source_contract.py "
            "--archive olist.zip "
            "--profile docs/source_profile.json"
        ),
    )

    prepare_raw_files = BashOperator(
        task_id="prepare_raw_files",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            f"{PYTHON_BIN} scripts/ingestion/prepare_olist_raw_files.py "
            "--archive olist.zip "
            "--profile docs/source_profile.json "
            f"--output-dir {LOCAL_RAW_DIR} "
            "--batch-date '{{ params.batch_date }}' "
            f"--batch-id '{LOCAL_BATCH_ID}' "
            f"--run-id '{LOCAL_RUN_ID}'"
        ),
    )

    generate_correction_feeds = BashOperator(
        task_id="generate_correction_feeds",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            f"{PYTHON_BIN} scripts/ingestion/generate_correction_feeds.py "
            "--archive olist.zip "
            f"--output-dir {LOCAL_RAW_DIR} "
            "--batch-date '{{ params.batch_date }}' "
            f"--batch-id '{LOCAL_BATCH_ID}' "
            f"--run-id '{LOCAL_RUN_ID}'"
        ),
    )

    load_raw_files_to_postgres = BashOperator(
        task_id="load_raw_files_to_postgres",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            f"{PYTHON_BIN} scripts/loading/load_raw_to_postgres.py "
            f"--raw-dir {LOCAL_RAW_DIR} "
            "--profile docs/source_profile.json "
            f"--bootstrap-sql-dir {POSTGRES_SQL_DIR} "
            "--batch-date '{{ params.batch_date }}' "
            f"--batch-id '{LOCAL_BATCH_ID}' "
            f"--run-id '{LOCAL_RUN_ID}'"
        ),
        env={
            **os.environ,
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "localhost"),
            "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB", "olist_analytics"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "olist"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "olist"),
        },
    )

    dbt_run_snapshot_inputs = BashOperator(
        task_id="dbt_run_snapshot_inputs",
        cwd=str(DBT_PROJECT_DIR),
        bash_command=(
            "dbt run --select staging intermediate --vars "
            "'{batch_date: \"{{ params.batch_date }}\"}'"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        cwd=str(DBT_PROJECT_DIR),
        bash_command="dbt snapshot --vars '{batch_date: \"{{ params.batch_date }}\"}'",
    )

    dbt_build_command = (
        "dbt build --exclude resource_type:snapshot --vars "
        "'{batch_date: \"{{ params.batch_date }}\", lookback_days: {{ params.lookback_days }}}'"
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        cwd=str(DBT_PROJECT_DIR),
        bash_command=(
            "{% if params.full_refresh %}"
            + dbt_build_command
            + " --full-refresh"
            + "{% else %}"
            + dbt_build_command
            + "{% endif %}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        cwd=str(DBT_PROJECT_DIR),
        bash_command=(
            "dbt test --vars "
            "'{batch_date: \"{{ params.batch_date }}\", lookback_days: {{ params.lookback_days }}}'"
        ),
    )

    end = EmptyOperator(task_id="end")

    _ = (
        start
        >> validate_source_contract
        >> prepare_raw_files
        >> generate_correction_feeds
        >> load_raw_files_to_postgres
        >> dbt_run_snapshot_inputs
        >> dbt_snapshot
        >> dbt_build
        >> dbt_test
        >> end
    )
