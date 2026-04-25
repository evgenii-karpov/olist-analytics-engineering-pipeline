"""Local Airflow DAG for the Olist Modern Data Stack project.

This DAG keeps the same logical pipeline as the AWS version, but uses a local
S3-shaped raw zone and PostgreSQL in Docker instead of S3 and Redshift.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import (
    BashOperator,  # pyright: ignore[reportMissingImports]
)
from airflow.providers.standard.operators.empty import (
    EmptyOperator,  # pyright: ignore[reportMissingImports]
)
from airflow.sdk import DAG  # pyright: ignore[reportMissingImports]

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


def batch_control_command(command: str, status: str | None = None) -> str:
    status_arg = f" --status {status}" if status else ""
    bootstrap_arg = (
        f" --bootstrap-sql-dir {POSTGRES_SQL_DIR}" if command == "start" else ""
    )
    return (
        f"{PYTHON_BIN} scripts/orchestration/batch_control.py {command} "
        "--batch-date '{{ params.batch_date }}' "
        f"--batch-id '{LOCAL_BATCH_ID}' "
        f"--run-id '{LOCAL_RUN_ID}' "
        f"--dag-id {DAG_ID} "
        f"--raw-dir {LOCAL_RAW_DIR}"
        f"{bootstrap_arg}"
        f"{status_arg}"
    )


def local_run_id(run_id: str) -> str:
    return run_id.replace(":", "_").replace("+", "_")


def mark_batch_failed(context: dict) -> None:
    params = context.get("params") or {}
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    exception = context.get("exception")
    batch_date = str(params.get("batch_date", "2018-09-01"))
    error_message = f"{task_id}: {exception}"[:65535]

    subprocess.run(
        [
            PYTHON_BIN,
            "scripts/orchestration/batch_control.py",
            "fail",
            "--batch-date",
            batch_date,
            "--batch-id",
            batch_date,
            "--run-id",
            local_run_id(str(context.get("run_id", "unknown_run"))),
            "--dag-id",
            DAG_ID,
            "--raw-dir",
            LOCAL_RAW_DIR,
            "--bootstrap-sql-dir",
            POSTGRES_SQL_DIR,
            "--error-message",
            error_message,
        ],
        cwd=str(PROJECT_ROOT),
        check=False,
    )


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": mark_batch_failed,
}


with DAG(
    dag_id=DAG_ID,
    description="Olist batch pipeline: local raw files, PostgreSQL load, and dbt transformations.",
    default_args=default_args,
    start_date=datetime(2016, 9, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "local", "postgres", "dbt"],
    params={
        "batch_date": "2018-09-01",
        "lookback_days": 3,
        "full_refresh": False,
        "dead_letter_max_rows": 10,
        "dead_letter_max_rate": 0.001,
    },
) as dag:
    start = EmptyOperator(task_id="start")

    start_batch = BashOperator(
        task_id="start_batch",
        cwd=str(PROJECT_ROOT),
        bash_command=batch_control_command("start"),
    )

    validate_source_contract = BashOperator(
        task_id="validate_source_contract",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            f"{PYTHON_BIN} scripts/utilities/validate_source_contract.py "
            "--archive olist.zip "
            "--profile docs/source_profile.json"
        ),
    )

    mark_source_validated = BashOperator(
        task_id="mark_source_validated",
        cwd=str(PROJECT_ROOT),
        bash_command=batch_control_command("mark", "SOURCE_VALIDATED"),
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
            f"--run-id '{LOCAL_RUN_ID}' "
            "--dead-letter-max-rows '{{ params.dead_letter_max_rows }}' "
            "--dead-letter-max-rate '{{ params.dead_letter_max_rate }}'"
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
            f"--run-id '{LOCAL_RUN_ID}' "
            "--dead-letter-max-rows '{{ params.dead_letter_max_rows }}' "
            "--dead-letter-max-rate '{{ params.dead_letter_max_rate }}'"
        ),
    )

    mark_raw_prepared = BashOperator(
        task_id="mark_raw_prepared",
        cwd=str(PROJECT_ROOT),
        bash_command=batch_control_command("mark", "RAW_PREPARED"),
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
            f"--run-id '{LOCAL_RUN_ID}' "
            f"--dag-id {DAG_ID}"
        ),
    )

    reconcile_raw_load = BashOperator(
        task_id="reconcile_raw_load",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            f"{PYTHON_BIN} scripts/quality/reconcile_batch.py "
            f"--raw-dir {LOCAL_RAW_DIR} "
            "--profile docs/source_profile.json "
            f"--bootstrap-sql-dir {POSTGRES_SQL_DIR} "
            "--batch-date '{{ params.batch_date }}' "
            f"--batch-id '{LOCAL_BATCH_ID}' "
            f"--run-id '{LOCAL_RUN_ID}' "
            f"--dag-id {DAG_ID}"
        ),
    )

    dbt_build_snapshot_inputs = BashOperator(
        task_id="dbt_build_snapshot_inputs",
        cwd=str(DBT_PROJECT_DIR),
        bash_command=(
            "dbt build --select staging intermediate --vars "
            "'{batch_date: \"{{ params.batch_date }}\"}'"
        ),
    )

    mark_snapshot_inputs_built = BashOperator(
        task_id="mark_snapshot_inputs_built",
        cwd=str(PROJECT_ROOT),
        bash_command=batch_control_command("mark", "DBT_SNAPSHOT_INPUTS_BUILT"),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        cwd=str(DBT_PROJECT_DIR),
        bash_command="dbt snapshot --vars '{batch_date: \"{{ params.batch_date }}\"}'",
    )

    mark_dbt_snapshotted = BashOperator(
        task_id="mark_dbt_snapshotted",
        cwd=str(PROJECT_ROOT),
        bash_command=batch_control_command("mark", "DBT_SNAPSHOTTED"),
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

    mark_dbt_built = BashOperator(
        task_id="mark_dbt_built",
        cwd=str(PROJECT_ROOT),
        bash_command=batch_control_command("mark", "DBT_BUILT"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        cwd=str(DBT_PROJECT_DIR),
        bash_command=(
            "dbt test --vars "
            "'{batch_date: \"{{ params.batch_date }}\", lookback_days: {{ params.lookback_days }}}'"
        ),
    )

    mark_tested = BashOperator(
        task_id="mark_tested",
        cwd=str(PROJECT_ROOT),
        bash_command=batch_control_command("mark", "TESTED"),
    )

    end = EmptyOperator(task_id="end")

    _ = (
        start
        >> start_batch
        >> validate_source_contract
        >> mark_source_validated
        >> prepare_raw_files
        >> generate_correction_feeds
        >> mark_raw_prepared
        >> load_raw_files_to_postgres
        >> reconcile_raw_load
        >> dbt_build_snapshot_inputs
        >> mark_snapshot_inputs_built
        >> dbt_snapshot
        >> mark_dbt_snapshotted
        >> dbt_build
        >> mark_dbt_built
        >> dbt_test
        >> mark_tested
        >> end
    )
