"""Local Airflow DAG for the Olist Modern Data Stack project.

This DAG keeps the same logical pipeline as the AWS version, but uses a local
S3-shaped raw zone and PostgreSQL in Docker instead of S3 and Redshift.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Param, dag, get_current_context, task, task_group
from airflow.sdk.exceptions import AirflowException

DAG_ID = "olist_modern_data_stack_local"


def resolve_project_root() -> Path:
    configured_root = os.environ.get("OLIST_PROJECT_ROOT")
    if configured_root:
        return Path(configured_root)

    for candidate in (Path.cwd(), *Path(__file__).resolve().parents):
        if (candidate / "pyproject.toml").exists():
            return candidate

    airflow_project_root = Path("/opt/airflow/project")
    if airflow_project_root.exists():
        return airflow_project_root

    return Path(__file__).resolve().parents[2]


PROJECT_ROOT = resolve_project_root()
PYTHON_BIN = os.environ.get("OLIST_PYTHON_BIN", "python")
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "olist_analytics"
LOCAL_RAW_DIR = "data/raw/olist"
POSTGRES_SQL_DIR = "infra/postgres"
# Runtime default for manual/demo runs. It is after all generated correction
# feed effective dates, so one batch sees the complete synthetic SCD2 scenario.
DEFAULT_DEMO_BATCH_DATE = "2018-09-01"


def local_run_id(run_id: str) -> str:
    return run_id.replace(":", "_").replace("+", "_")


def run_project_command(command: list[str]) -> None:
    subprocess.run(command, cwd=str(PROJECT_ROOT), check=True)


def current_batch_identifiers() -> tuple[Mapping[str, Any], str, str]:
    context = get_current_context()
    params = context.get("params")
    run_id = context.get("run_id")
    if not isinstance(params, Mapping):
        raise AirflowException("Airflow task context is missing params")
    if run_id is None:
        raise AirflowException("Airflow task context is missing run_id")

    batch_date = str(params["batch_date"])
    return params, batch_date, local_run_id(str(run_id))


def batch_control_args(
    command: str,
    batch_date: str,
    run_id: str,
    status: str | None = None,
) -> list[str]:
    args = [
        PYTHON_BIN,
        "scripts/orchestration/batch_control.py",
        command,
        "--batch-date",
        batch_date,
        "--batch-id",
        batch_date,
        "--run-id",
        run_id,
        "--dag-id",
        DAG_ID,
        "--raw-dir",
        LOCAL_RAW_DIR,
    ]
    if command == "start":
        args.extend(["--bootstrap-sql-dir", POSTGRES_SQL_DIR])
    if status:
        args.extend(["--status", status])
    return args


def mark_batch_failed(context: dict) -> None:
    params = context.get("params") or {}
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    exception = context.get("exception")
    batch_date = str(params.get("batch_date", DEFAULT_DEMO_BATCH_DATE))
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


dag_params = {
    "batch_date": Param(
        DEFAULT_DEMO_BATCH_DATE,
        type="string",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Batch date in YYYY-MM-DD format.",
    ),
    "lookback_days": Param(
        3,
        type="integer",
        minimum=0,
        maximum=365,
        description="Late-arriving data lookback window for incremental dbt models.",
    ),
    "full_refresh": Param(
        False,
        type="boolean",
        description="Run dbt build with --full-refresh.",
    ),
    "dead_letter_max_rows": Param(
        10,
        type="integer",
        minimum=0,
        maximum=100000,
        description="Maximum accepted dead-letter row count.",
    ),
    "dead_letter_max_rate": Param(
        0.001,
        type="number",
        minimum=0,
        maximum=1,
        description="Maximum accepted dead-letter rate.",
    ),
}


@dag(
    dag_id=DAG_ID,
    description="Olist batch pipeline: local raw files, PostgreSQL load, and dbt transformations.",
    default_args=default_args,
    start_date=datetime(2016, 9, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "local", "postgres", "dbt"],
    params=dag_params,
)
def olist_modern_data_stack_local():
    start = EmptyOperator(task_id="start")

    @task
    def start_batch() -> None:
        _, batch_date, run_id = current_batch_identifiers()
        run_project_command(batch_control_args("start", batch_date, run_id))

    @task
    def mark_batch_status(status: str) -> None:
        _, batch_date, run_id = current_batch_identifiers()
        run_project_command(
            batch_control_args(
                "mark",
                batch_date,
                run_id,
                status=status,
            )
        )

    @task_group(group_id="raw_preparation")
    def raw_preparation():
        @task
        def validate_source_contract() -> None:
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/utilities/validate_source_contract.py",
                    "--archive",
                    "olist.zip",
                    "--profile",
                    "docs/source_profile.json",
                ]
            )

        @task
        def prepare_raw_files() -> None:
            params, batch_date, run_id = current_batch_identifiers()
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/ingestion/prepare_olist_raw_files.py",
                    "--archive",
                    "olist.zip",
                    "--profile",
                    "docs/source_profile.json",
                    "--output-dir",
                    LOCAL_RAW_DIR,
                    "--batch-date",
                    batch_date,
                    "--batch-id",
                    batch_date,
                    "--run-id",
                    run_id,
                    "--dead-letter-max-rows",
                    str(params["dead_letter_max_rows"]),
                    "--dead-letter-max-rate",
                    str(params["dead_letter_max_rate"]),
                ]
            )

        @task
        def generate_correction_feeds() -> None:
            params, batch_date, run_id = current_batch_identifiers()
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/ingestion/generate_correction_feeds.py",
                    "--archive",
                    "olist.zip",
                    "--output-dir",
                    LOCAL_RAW_DIR,
                    "--batch-date",
                    batch_date,
                    "--batch-id",
                    batch_date,
                    "--run-id",
                    run_id,
                    "--dead-letter-max-rows",
                    str(params["dead_letter_max_rows"]),
                    "--dead-letter-max-rate",
                    str(params["dead_letter_max_rate"]),
                ]
            )

        source_contract = validate_source_contract()
        source_validated = mark_batch_status.override(task_id="mark_source_validated")(
            "SOURCE_VALIDATED"
        )
        raw_files = prepare_raw_files()
        correction_feeds = generate_correction_feeds()
        raw_prepared = mark_batch_status.override(task_id="mark_raw_prepared")(
            "RAW_PREPARED"
        )

        _ = source_contract >> source_validated
        _ = source_validated >> [raw_files, correction_feeds] >> raw_prepared

    @task_group(group_id="raw_load_quality")
    def raw_load_quality():
        @task
        def load_raw_files_to_postgres() -> None:
            _, batch_date, run_id = current_batch_identifiers()
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/loading/load_raw_to_postgres.py",
                    "--raw-dir",
                    LOCAL_RAW_DIR,
                    "--profile",
                    "docs/source_profile.json",
                    "--bootstrap-sql-dir",
                    POSTGRES_SQL_DIR,
                    "--batch-date",
                    batch_date,
                    "--batch-id",
                    batch_date,
                    "--run-id",
                    run_id,
                    "--dag-id",
                    DAG_ID,
                ]
            )

        @task
        def reconcile_raw_load() -> None:
            _, batch_date, run_id = current_batch_identifiers()
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/quality/reconcile_batch.py",
                    "--raw-dir",
                    LOCAL_RAW_DIR,
                    "--profile",
                    "docs/source_profile.json",
                    "--bootstrap-sql-dir",
                    POSTGRES_SQL_DIR,
                    "--batch-date",
                    batch_date,
                    "--batch-id",
                    batch_date,
                    "--run-id",
                    run_id,
                    "--dag-id",
                    DAG_ID,
                ]
            )

        _ = load_raw_files_to_postgres() >> reconcile_raw_load()

    @task_group(group_id="dbt_transformations")
    def dbt_transformations():
        dbt_build_snapshot_inputs = BashOperator(
            task_id="dbt_build_snapshot_inputs",
            cwd=str(DBT_PROJECT_DIR),
            bash_command=(
                "dbt build --select staging intermediate "
                "--indirect-selection cautious --vars "
                "'{batch_date: \"{{ params.batch_date }}\"}'"
            ),
        )

        mark_snapshot_inputs_built = mark_batch_status.override(
            task_id="mark_snapshot_inputs_built"
        )("DBT_SNAPSHOT_INPUTS_BUILT")

        dbt_snapshot = BashOperator(
            task_id="dbt_snapshot",
            cwd=str(DBT_PROJECT_DIR),
            bash_command="dbt snapshot --vars '{batch_date: \"{{ params.batch_date }}\"}'",
        )

        mark_dbt_snapshotted = mark_batch_status.override(
            task_id="mark_dbt_snapshotted"
        )("DBT_SNAPSHOTTED")

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

        mark_dbt_built = mark_batch_status.override(task_id="mark_dbt_built")(
            "DBT_BUILT"
        )

        dbt_test = BashOperator(
            task_id="dbt_test",
            cwd=str(DBT_PROJECT_DIR),
            bash_command=(
                "dbt test --vars "
                "'{batch_date: \"{{ params.batch_date }}\", lookback_days: {{ params.lookback_days }}}'"
            ),
        )

        mark_tested = mark_batch_status.override(task_id="mark_tested")("TESTED")

        _ = (
            dbt_build_snapshot_inputs
            >> mark_snapshot_inputs_built
            >> dbt_snapshot
            >> mark_dbt_snapshotted
            >> dbt_build
            >> mark_dbt_built
            >> dbt_test
            >> mark_tested
        )

    end = EmptyOperator(task_id="end")

    _ = (
        start
        >> start_batch()
        >> raw_preparation()
        >> raw_load_quality()
        >> dbt_transformations()
        >> end
    )


olist_modern_data_stack_local()
