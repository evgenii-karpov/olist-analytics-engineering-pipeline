"""AWS Airflow DAG for the Olist Modern Data Stack project.

This DAG mirrors the local pipeline structure, but stages prepared files for S3
and loads them into Redshift before running dbt on the Redshift target.
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

DAG_ID = "olist_modern_data_stack_aws"


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
DEFAULT_SOURCE_ARCHIVE = "olist.zip"
DEFAULT_SOURCE_PROFILE = "docs/source_profile.json"
DEFAULT_PREPARED_DIR_TEMPLATE = "data/prepared/{ds_nodash}"
DEFAULT_S3_PREFIX = "olist"
DEFAULT_DBT_TARGET = "redshift"
REDSHIFT_SQL_DIR = "infra/redshift"
DEFAULT_RETRIES = int(os.environ.get("OLIST_AIRFLOW_RETRIES", "2"))
DEFAULT_RETRY_DELAY_SECONDS = int(
    os.environ.get("OLIST_AIRFLOW_RETRY_DELAY_SECONDS", "300")
)
# Runtime default for manual/demo runs. It is after all generated correction
# feed effective dates, so one batch sees the complete synthetic SCD2 scenario.
DEFAULT_DEMO_BATCH_DATE = "2018-09-01"


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise AirflowException(f"Missing required environment variable: {name}")
    return value


def param_or_env(params: Mapping[str, Any], param_name: str, env_name: str) -> str:
    value = str(params.get(param_name) or os.environ.get(env_name, ""))
    if not value:
        raise AirflowException(
            f"Missing required DAG param {param_name!r} or environment variable {env_name}"
        )
    return value


def run_project_command(command: list[str], env: dict[str, str] | None = None) -> None:
    subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        check=True,
        env=env,
    )


def current_batch_context() -> tuple[Mapping[str, Any], str, str, str]:
    context = get_current_context()
    params = context.get("params")
    ds_nodash = context.get("ds_nodash")
    run_id = context.get("run_id")
    if not isinstance(params, Mapping):
        raise AirflowException("Airflow task context is missing params")
    if ds_nodash is None:
        raise AirflowException("Airflow task context is missing ds_nodash")
    if run_id is None:
        raise AirflowException("Airflow task context is missing run_id")

    batch_date = str(params["batch_date"])
    artifact_dir = str(params["prepared_dir_template"]).format(ds_nodash=ds_nodash)
    return params, batch_date, str(run_id), artifact_dir


def redshift_batch_control_env() -> dict[str, str]:
    env = os.environ.copy()
    env["WAREHOUSE_HOST"] = required_env("REDSHIFT_HOST")
    env["WAREHOUSE_PORT"] = os.environ.get("REDSHIFT_PORT", "5439")
    env["WAREHOUSE_DB"] = required_env("REDSHIFT_DATABASE")
    env["WAREHOUSE_USER"] = required_env("REDSHIFT_USER")
    env["WAREHOUSE_PASSWORD"] = required_env("REDSHIFT_PASSWORD")
    return env


def batch_control_args(
    command: str,
    batch_date: str,
    run_id: str,
    raw_dir: str,
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
        raw_dir,
    ]
    if command == "start":
        args.extend(["--bootstrap-sql-dir", REDSHIFT_SQL_DIR])
    if status:
        args.extend(["--status", status])
    return args


def mark_batch_failed(context: dict) -> None:
    params = context.get("params") or {}
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    exception = context.get("exception")
    batch_date = str(params.get("batch_date", DEFAULT_DEMO_BATCH_DATE))
    ds_nodash = str(context.get("ds_nodash", "unknown_ds"))
    artifact_dir = str(
        params.get("prepared_dir_template", DEFAULT_PREPARED_DIR_TEMPLATE)
    )
    raw_dir = artifact_dir.format(ds_nodash=ds_nodash)
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
            str(context.get("run_id", "unknown_run")),
            "--dag-id",
            DAG_ID,
            "--raw-dir",
            raw_dir,
            "--bootstrap-sql-dir",
            REDSHIFT_SQL_DIR,
            "--error-message",
            error_message,
        ],
        cwd=str(PROJECT_ROOT),
        check=False,
        env=redshift_batch_control_env(),
    )


default_args = {
    "owner": "data-engineering",
    "retries": DEFAULT_RETRIES,
    "retry_delay": timedelta(seconds=DEFAULT_RETRY_DELAY_SECONDS),
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
    "source_archive": Param(
        DEFAULT_SOURCE_ARCHIVE,
        type="string",
        description="Path to the source Olist zip archive.",
    ),
    "source_profile": Param(
        DEFAULT_SOURCE_PROFILE,
        type="string",
        description="Path to the source profile JSON file.",
    ),
    "prepared_dir_template": Param(
        DEFAULT_PREPARED_DIR_TEMPLATE,
        type="string",
        description="Local prepared-file directory template. Supports {ds_nodash}.",
    ),
    "s3_bucket": Param(
        os.environ.get("OLIST_S3_BUCKET", ""),
        type="string",
        description="S3 bucket for prepared raw files. Falls back to OLIST_S3_BUCKET.",
    ),
    "s3_prefix": Param(
        os.environ.get("OLIST_S3_PREFIX", DEFAULT_S3_PREFIX),
        type="string",
        description="S3 prefix for prepared raw files.",
    ),
    "aws_region": Param(
        os.environ.get("AWS_REGION", "us-east-1"),
        type="string",
        description="AWS region used by Redshift COPY.",
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
    description="Olist batch pipeline: S3 raw files, Redshift load, and dbt transformations.",
    default_args=default_args,
    start_date=datetime(2016, 9, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "aws", "s3", "redshift", "dbt"],
    params=dag_params,
)
def olist_modern_data_stack_aws():
    start = EmptyOperator(task_id="start")

    @task
    def start_batch() -> None:
        _, batch_date, run_id, artifact_dir = current_batch_context()
        run_project_command(
            batch_control_args("start", batch_date, run_id, artifact_dir),
            env=redshift_batch_control_env(),
        )

    @task
    def mark_batch_status(status: str) -> None:
        _, batch_date, run_id, artifact_dir = current_batch_context()
        run_project_command(
            batch_control_args(
                "mark",
                batch_date,
                run_id,
                artifact_dir,
                status=status,
            ),
            env=redshift_batch_control_env(),
        )

    @task_group(group_id="raw_preparation")
    def raw_preparation():
        @task
        def validate_source_contract() -> None:
            params, _, _, _ = current_batch_context()
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/utilities/validate_source_contract.py",
                    "--archive",
                    str(params["source_archive"]),
                    "--profile",
                    str(params["source_profile"]),
                ]
            )

        @task
        def upload_raw_files_to_s3() -> None:
            params, batch_date, run_id, artifact_dir = current_batch_context()
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/ingestion/ingest_olist_to_s3.py",
                    "--archive",
                    str(params["source_archive"]),
                    "--profile",
                    str(params["source_profile"]),
                    "--output-dir",
                    artifact_dir,
                    "--batch-date",
                    batch_date,
                    "--batch-id",
                    batch_date,
                    "--run-id",
                    run_id,
                    "--s3-bucket",
                    param_or_env(params, "s3_bucket", "OLIST_S3_BUCKET"),
                    "--s3-prefix",
                    str(params["s3_prefix"]),
                    "--dead-letter-max-rows",
                    str(params["dead_letter_max_rows"]),
                    "--dead-letter-max-rate",
                    str(params["dead_letter_max_rate"]),
                    "--upload",
                ]
            )

        @task
        def generate_correction_feeds() -> None:
            params, batch_date, run_id, artifact_dir = current_batch_context()
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/ingestion/generate_correction_feeds.py",
                    "--archive",
                    str(params["source_archive"]),
                    "--output-dir",
                    artifact_dir,
                    "--batch-date",
                    batch_date,
                    "--batch-id",
                    batch_date,
                    "--run-id",
                    run_id,
                    "--s3-bucket",
                    param_or_env(params, "s3_bucket", "OLIST_S3_BUCKET"),
                    "--s3-prefix",
                    str(params["s3_prefix"]),
                    "--dead-letter-max-rows",
                    str(params["dead_letter_max_rows"]),
                    "--dead-letter-max-rate",
                    str(params["dead_letter_max_rate"]),
                    "--upload",
                ]
            )

        source_contract = validate_source_contract()
        source_validated = mark_batch_status.override(task_id="mark_source_validated")(
            "SOURCE_VALIDATED"
        )
        raw_files = upload_raw_files_to_s3()
        correction_feeds = generate_correction_feeds()
        raw_prepared = mark_batch_status.override(task_id="mark_raw_prepared")(
            "RAW_PREPARED"
        )

        _ = source_contract >> source_validated
        _ = source_validated >> [raw_files, correction_feeds] >> raw_prepared

    @task_group(group_id="raw_load_quality")
    def raw_load_quality():
        @task
        def load_raw_files_to_redshift() -> None:
            params, batch_date, run_id, artifact_dir = current_batch_context()
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/loading/load_raw_to_redshift.py",
                    "--raw-dir",
                    artifact_dir,
                    "--profile",
                    str(params["source_profile"]),
                    "--bootstrap-sql-dir",
                    REDSHIFT_SQL_DIR,
                    "--batch-date",
                    batch_date,
                    "--batch-id",
                    batch_date,
                    "--run-id",
                    run_id,
                    "--dag-id",
                    DAG_ID,
                    "--s3-bucket",
                    param_or_env(params, "s3_bucket", "OLIST_S3_BUCKET"),
                    "--s3-prefix",
                    str(params["s3_prefix"]),
                    "--aws-region",
                    str(params["aws_region"]),
                ]
            )

        @task
        def reconcile_raw_load() -> None:
            params, batch_date, run_id, artifact_dir = current_batch_context()
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/quality/reconcile_batch.py",
                    "--raw-dir",
                    artifact_dir,
                    "--profile",
                    str(params["source_profile"]),
                    "--bootstrap-sql-dir",
                    REDSHIFT_SQL_DIR,
                    "--batch-date",
                    batch_date,
                    "--batch-id",
                    batch_date,
                    "--run-id",
                    run_id,
                    "--dag-id",
                    DAG_ID,
                ],
                env=redshift_batch_control_env(),
            )

        _ = load_raw_files_to_redshift() >> reconcile_raw_load()

    @task_group(group_id="dbt_transformations")
    def dbt_transformations():
        dbt_build_command = (
            "dbt build --vars "
            "'{batch_date: \"{{ params.batch_date }}\", lookback_days: {{ params.lookback_days }}}'"
        )

        dbt_build = BashOperator(
            task_id="dbt_build",
            cwd=str(DBT_PROJECT_DIR),
            env={**os.environ, "DBT_TARGET": DEFAULT_DBT_TARGET},
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

        _ = dbt_build >> mark_dbt_built

    end = EmptyOperator(task_id="end")

    _ = (
        start
        >> start_batch()
        >> raw_preparation()
        >> raw_load_quality()
        >> dbt_transformations()
        >> end
    )


olist_modern_data_stack_aws()
