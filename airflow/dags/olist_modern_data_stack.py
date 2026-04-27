"""Airflow DAG skeleton for the Olist Modern Data Stack project."""

from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Param, dag, get_current_context, task, task_group

DAG_ID = "olist_modern_data_stack"


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
SOURCE_PROFILE_PATH = PROJECT_ROOT / "docs" / "source_profile.json"
# Runtime default for manual/demo runs. It is after all generated correction
# feed effective dates, so one batch sees the complete synthetic SCD2 scenario.
DEFAULT_DEMO_BATCH_DATE = "2018-09-01"


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise AirflowException(f"Missing required environment variable: {name}")
    return value


def load_entities() -> list[str]:
    profile = json.loads(SOURCE_PROFILE_PATH.read_text(encoding="utf-8"))
    correction_entities = [
        "customer_profile_changes",
        "product_attribute_changes",
    ]
    return [entity["entity_name"] for entity in profile] + correction_entities


def run_project_command(command: list[str]) -> None:
    subprocess.run(command, cwd=str(PROJECT_ROOT), check=True)


def copy_raw_files_to_redshift_callable(**context) -> None:
    """Run Redshift COPY for every raw Olist entity."""
    try:
        import psycopg2
    except ImportError as exc:
        raise AirflowException("psycopg2 is required for Redshift COPY tasks") from exc

    batch_date = context["params"]["batch_date"]
    run_id = context["run_id"]

    bucket = required_env("OLIST_S3_BUCKET")
    prefix = os.environ.get("OLIST_S3_PREFIX", "olist").strip("/")
    aws_region = os.environ.get("AWS_REGION", "us-east-1")
    iam_role_arn = required_env("REDSHIFT_COPY_IAM_ROLE_ARN")

    connection = psycopg2.connect(
        host=required_env("REDSHIFT_HOST"),
        port=int(os.environ.get("REDSHIFT_PORT", "5439")),
        dbname=required_env("REDSHIFT_DATABASE"),
        user=required_env("REDSHIFT_USER"),
        password=required_env("REDSHIFT_PASSWORD"),
    )
    connection.autocommit = True

    copy_template = """
copy raw.{entity_name}
from '{source_uri}'
iam_role '{iam_role_arn}'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
emptyasnull
blanksasnull
acceptinvchars
region '{aws_region}';
"""

    started_at = context["logical_date"].replace(tzinfo=None)

    try:
        with connection.cursor() as cursor:
            for entity_name in load_entities():
                source_uri = (
                    f"s3://{bucket}/{prefix}/raw/{entity_name}/"
                    f"batch_date={batch_date}/run_id={run_id}/"
                )
                cursor.execute(
                    f"delete from raw.{entity_name} where _batch_id = %s;",
                    (run_id,),
                )
                cursor.execute(
                    """
                    delete from audit.load_runs
                    where load_run_id = %s
                      and entity_name = %s;
                    """,
                    (run_id, entity_name),
                )
                cursor.execute(
                    copy_template.format(
                        entity_name=entity_name,
                        source_uri=source_uri,
                        iam_role_arn=iam_role_arn,
                        aws_region=aws_region,
                    )
                )
                cursor.execute(
                    f"""
                    insert into audit.load_runs (
                        load_run_id,
                        batch_id,
                        entity_name,
                        source_uri,
                        target_table,
                        status,
                        rows_loaded,
                        started_at,
                        finished_at,
                        error_message
                    )
                    select
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        'SUCCESS',
                        count(*),
                        %s,
                        getdate(),
                        null
                    from raw.{entity_name}
                    where _batch_id = %s;
                    """,
                    (
                        run_id,
                        run_id,
                        entity_name,
                        source_uri,
                        f"raw.{entity_name}",
                        started_at,
                        run_id,
                    ),
                )
    finally:
        connection.close()


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
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
}


@dag(
    dag_id=DAG_ID,
    description="Olist batch pipeline: S3 raw load, Redshift COPY, and dbt transformations.",
    default_args=default_args,
    start_date=datetime(2016, 9, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["olist", "s3", "redshift", "dbt"],
    params=dag_params,
)
def olist_modern_data_stack():
    start = EmptyOperator(task_id="start")

    @task_group(group_id="raw_ingestion")
    def raw_ingestion():
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
        def upload_raw_files_to_s3() -> None:
            context = get_current_context()
            params = context["params"]
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/ingestion/ingest_olist_to_s3.py",
                    "--archive",
                    "olist.zip",
                    "--profile",
                    "docs/source_profile.json",
                    "--output-dir",
                    f"data/prepared/{context['ds_nodash']}",
                    "--batch-date",
                    str(params["batch_date"]),
                    "--run-id",
                    str(context["run_id"]),
                    "--s3-bucket",
                    required_env("OLIST_S3_BUCKET"),
                    "--s3-prefix",
                    os.environ.get("OLIST_S3_PREFIX", "olist"),
                    "--upload",
                ]
            )

        @task
        def generate_and_upload_correction_feeds() -> None:
            context = get_current_context()
            params = context["params"]
            run_project_command(
                [
                    PYTHON_BIN,
                    "scripts/ingestion/generate_correction_feeds.py",
                    "--archive",
                    "olist.zip",
                    "--output-dir",
                    f"data/prepared/{context['ds_nodash']}",
                    "--batch-date",
                    str(params["batch_date"]),
                    "--run-id",
                    str(context["run_id"]),
                    "--s3-bucket",
                    required_env("OLIST_S3_BUCKET"),
                    "--s3-prefix",
                    os.environ.get("OLIST_S3_PREFIX", "olist"),
                    "--upload",
                ]
            )

        @task
        def copy_raw_files_to_redshift() -> None:
            copy_raw_files_to_redshift_callable(**get_current_context())

        source_validated = validate_source_contract()
        raw_uploaded = upload_raw_files_to_s3()
        corrections_uploaded = generate_and_upload_correction_feeds()
        redshift_loaded = copy_raw_files_to_redshift()

        _ = source_validated >> [raw_uploaded, corrections_uploaded] >> redshift_loaded

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

        _ = dbt_build_snapshot_inputs >> dbt_snapshot >> dbt_build >> dbt_test

    end = EmptyOperator(task_id="end")

    _ = start >> raw_ingestion() >> dbt_transformations() >> end


olist_modern_data_stack()
