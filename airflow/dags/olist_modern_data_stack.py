"""Airflow DAG skeleton for the Olist Modern Data Stack project."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG  # pyright: ignore[reportMissingImports,reportMissingModuleSource]
from airflow.exceptions import AirflowException  # pyright: ignore[reportMissingImports,reportMissingModuleSource]
from airflow.operators.bash import BashOperator  # pyright: ignore[reportMissingImports,reportMissingModuleSource]
from airflow.operators.empty import EmptyOperator  # pyright: ignore[reportMissingImports,reportMissingModuleSource]
from airflow.operators.python import PythonOperator  # pyright: ignore[reportMissingImports,reportMissingModuleSource]


DAG_ID = "olist_modern_data_stack"
PROJECT_ROOT = Path(
    os.environ.get("OLIST_PROJECT_ROOT", Path(__file__).resolve().parents[2])
)
PYTHON_BIN = os.environ.get("OLIST_PYTHON_BIN", "python")
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "olist_analytics"
SOURCE_PROFILE_PATH = PROJECT_ROOT / "docs" / "source_profile.json"


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


def copy_raw_files_to_redshift(**context) -> None:
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

    started_at = datetime.utcnow()

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
                    """
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
                    """.format(entity_name=entity_name),
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


with DAG(
    dag_id=DAG_ID,
    description="Olist batch pipeline: S3 raw load, Redshift COPY, and dbt transformations.",
    default_args=default_args,
    start_date=datetime(2016, 9, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["olist", "s3", "redshift", "dbt"],
    # Runtime defaults for manual/demo runs. 2018-09-01 is after the
    # generated correction feed effective dates, so the sample SCD2 flow
    # has visible changes without requiring a backfill sequence.
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

    upload_raw_files_to_s3 = BashOperator(
        task_id="upload_raw_files_to_s3",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            f"{PYTHON_BIN} scripts/ingestion/ingest_olist_to_s3.py "
            "--archive olist.zip "
            "--profile docs/source_profile.json "
            "--output-dir data/prepared/{{{{ ds_nodash }}}} "
            "--batch-date '{{{{ params.batch_date }}}}' "
            "--run-id '{{{{ run_id }}}}' "
            '--s3-bucket "$OLIST_S3_BUCKET" '
            '--s3-prefix "$OLIST_S3_PREFIX" '
            "--upload"
        ),
        env={
            **os.environ,
            "AWS_PROFILE": os.environ.get("AWS_PROFILE", "default"),
            "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
            "OLIST_S3_BUCKET": os.environ.get("OLIST_S3_BUCKET", ""),
            "OLIST_S3_PREFIX": os.environ.get("OLIST_S3_PREFIX", "olist"),
        },
    )

    generate_and_upload_correction_feeds = BashOperator(
        task_id="generate_and_upload_correction_feeds",
        cwd=str(PROJECT_ROOT),
        bash_command=(
            f"{PYTHON_BIN} scripts/ingestion/generate_correction_feeds.py "
            "--archive olist.zip "
            "--output-dir data/prepared/{{{{ ds_nodash }}}} "
            "--batch-date '{{{{ params.batch_date }}}}' "
            "--run-id '{{{{ run_id }}}}' "
            '--s3-bucket "$OLIST_S3_BUCKET" '
            '--s3-prefix "$OLIST_S3_PREFIX" '
            "--upload"
        ),
        env={
            **os.environ,
            "AWS_PROFILE": os.environ.get("AWS_PROFILE", "default"),
            "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
            "OLIST_S3_BUCKET": os.environ.get("OLIST_S3_BUCKET", ""),
            "OLIST_S3_PREFIX": os.environ.get("OLIST_S3_PREFIX", "olist"),
        },
    )

    copy_raw_files = PythonOperator(
        task_id="copy_raw_files_to_redshift",
        python_callable=copy_raw_files_to_redshift,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        cwd=str(DBT_PROJECT_DIR),
        bash_command="dbt snapshot --vars '{batch_date: \"{{ params.batch_date }}\"}'",
    )

    dbt_build_command = (
        "dbt build --vars "
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
        >> upload_raw_files_to_s3
        >> generate_and_upload_correction_feeds
        >> copy_raw_files
        >> dbt_snapshot
        >> dbt_build
        >> dbt_test
        >> end
    )
