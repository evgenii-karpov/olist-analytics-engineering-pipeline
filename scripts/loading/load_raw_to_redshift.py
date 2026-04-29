"""Load prepared S3-backed raw files into Redshift raw tables."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection

from scripts.loading.load_raw_to_postgres import (
    RAW_SCHEMA,
    DeadLetterManifestEntry,
    RawLoadSpec,
    execute_sql_files,
    fetch_one,
    load_dead_letter_manifest_entries,
    load_specs,
)
from scripts.orchestration.batch_control import BatchRunContext, mark_batch_status


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0, tzinfo=None)


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def redshift_connection(args: argparse.Namespace) -> PgConnection:
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
    )


def source_uri_for(
    bucket: str,
    prefix: str,
    entity_name: str,
    batch_date: str,
    run_id: str,
) -> str:
    normalized_prefix = prefix.strip("/")
    prefix_part = f"{normalized_prefix}/" if normalized_prefix else ""
    return (
        f"s3://{bucket}/{prefix_part}raw/{entity_name}/"
        f"batch_date={batch_date}/run_id={run_id}/"
    )


def copy_into_raw_table(
    connection: PgConnection,
    spec: RawLoadSpec,
    source_uri: str,
    aws_region: str,
    iam_role_arn: str,
) -> None:
    copy_statement = sql.SQL(
        """
        copy {}.{}
        from %s
        iam_role %s
        csv
        gzip
        ignoreheader 1
        timeformat 'auto'
        dateformat 'auto'
        emptyasnull
        blanksasnull
        acceptinvchars
        region %s;
        """
    ).format(sql.Identifier(RAW_SCHEMA), sql.Identifier(spec.entity_name))

    with connection.cursor() as cursor:
        cursor.execute(copy_statement, (source_uri, iam_role_arn, aws_region))


def record_success(
    connection: PgConnection,
    spec: RawLoadSpec,
    batch_id: str,
    run_id: str,
    source_uri: str,
    started_at: datetime,
) -> None:
    with connection.cursor() as cursor:
        count_statement = sql.SQL(
            "select count(*) from {}.{} where _batch_id = %s"
        ).format(sql.Identifier(RAW_SCHEMA), sql.Identifier(spec.entity_name))
        cursor.execute(count_statement, (batch_id,))
        rows_loaded = fetch_one(cursor)[0]
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
            values (%s, %s, %s, %s, %s, 'SUCCESS', %s, %s, current_timestamp, null);
            """,
            (
                run_id,
                batch_id,
                spec.entity_name,
                source_uri,
                f"{RAW_SCHEMA}.{spec.entity_name}",
                rows_loaded,
                started_at,
            ),
        )


def record_dead_letter_event(
    connection: PgConnection,
    spec: RawLoadSpec,
    batch_id: str,
    run_id: str,
    manifest_entry: DeadLetterManifestEntry | None,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            delete from audit.dead_letter_events
            where batch_id = %s
              and entity_name = %s;
            """,
            (batch_id, spec.entity_name),
        )

        if manifest_entry is None or manifest_entry.failed_rows == 0:
            return

        cursor.execute(
            """
            insert into audit.dead_letter_events (
                dead_letter_event_id,
                batch_id,
                load_run_id,
                entity_name,
                source_uri,
                dead_letter_uri,
                total_rows,
                valid_rows,
                failed_rows,
                threshold_max_rows,
                threshold_max_rate,
                reason_summary,
                created_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, current_timestamp);
            """,
            (
                f"{batch_id}:{run_id}:{spec.entity_name}",
                batch_id,
                run_id,
                spec.entity_name,
                manifest_entry.source_uri,
                manifest_entry.dead_letter_uri,
                manifest_entry.total_rows,
                manifest_entry.valid_rows,
                manifest_entry.failed_rows,
                manifest_entry.threshold_max_rows,
                manifest_entry.threshold_max_rate,
                manifest_entry.reason_summary[:65535],
            ),
        )


def record_failure(
    connection: PgConnection,
    spec: RawLoadSpec,
    batch_id: str,
    run_id: str,
    source_uri: str,
    started_at: datetime,
    error: Exception,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            delete from audit.load_runs
            where batch_id = %s
              and entity_name = %s;
            """,
            (batch_id, spec.entity_name),
        )
        cursor.execute(
            """
            delete from audit.dead_letter_events
            where batch_id = %s
              and entity_name = %s;
            """,
            (batch_id, spec.entity_name),
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
            values (%s, %s, %s, %s, %s, 'FAILED', 0, %s, current_timestamp, %s);
            """,
            (
                run_id,
                batch_id,
                spec.entity_name,
                source_uri,
                f"{RAW_SCHEMA}.{spec.entity_name}",
                started_at,
                str(error)[:65535],
            ),
        )
    connection.commit()


def load_one_spec(
    connection: PgConnection,
    spec: RawLoadSpec,
    batch_date: str,
    batch_id: str,
    run_id: str,
    bucket: str,
    prefix: str,
    aws_region: str,
    iam_role_arn: str,
    dead_letter_entry: DeadLetterManifestEntry | None,
) -> None:
    source_uri = source_uri_for(bucket, prefix, spec.entity_name, batch_date, run_id)
    started_at = utc_now()
    try:
        with connection.cursor() as cursor:
            delete_statement = sql.SQL("delete from {}.{} where _batch_id = %s").format(
                sql.Identifier(RAW_SCHEMA), sql.Identifier(spec.entity_name)
            )
            cursor.execute(delete_statement, (batch_id,))
            cursor.execute(
                """
                delete from audit.load_runs
                where batch_id = %s
                  and entity_name = %s;
                """,
                (batch_id, spec.entity_name),
            )

        record_dead_letter_event(
            connection=connection,
            spec=spec,
            batch_id=batch_id,
            run_id=run_id,
            manifest_entry=dead_letter_entry,
        )
        copy_into_raw_table(connection, spec, source_uri, aws_region, iam_role_arn)
        record_success(connection, spec, batch_id, run_id, source_uri, started_at)
        connection.commit()
        print(f"Loaded {spec.entity_name} from {source_uri}")
    except Exception as exc:
        connection.rollback()
        record_failure(connection, spec, batch_id, run_id, source_uri, started_at, exc)
        raise


def load_all(
    connection: PgConnection,
    specs: list[RawLoadSpec],
    batch_date: str,
    batch_id: str,
    run_id: str,
    bucket: str,
    prefix: str,
    aws_region: str,
    iam_role_arn: str,
    dead_letter_entries: dict[str, DeadLetterManifestEntry],
) -> None:
    for spec in specs:
        load_one_spec(
            connection=connection,
            spec=spec,
            batch_date=batch_date,
            batch_id=batch_id,
            run_id=run_id,
            bucket=bucket,
            prefix=prefix,
            aws_region=aws_region,
            iam_role_arn=iam_role_arn,
            dead_letter_entry=dead_letter_entries.get(spec.entity_name),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default="data/prepared")
    parser.add_argument("--profile", default="docs/source_profile.json")
    parser.add_argument("--bootstrap-sql-dir")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--batch-id")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dag-id")
    parser.add_argument("--disable-batch-control", action="store_true")
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument(
        "--s3-prefix", default=os.environ.get("OLIST_S3_PREFIX", "olist")
    )
    parser.add_argument(
        "--aws-region", default=os.environ.get("AWS_REGION", "us-east-1")
    )
    parser.add_argument("--host", default=os.environ.get("REDSHIFT_HOST"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("REDSHIFT_PORT", "5439"))
    )
    parser.add_argument("--database", default=os.environ.get("REDSHIFT_DATABASE"))
    parser.add_argument("--user", default=os.environ.get("REDSHIFT_USER"))
    parser.add_argument("--password", default=os.environ.get("REDSHIFT_PASSWORD"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    batch_id = args.batch_id or args.batch_date
    connection = redshift_connection(args)
    try:
        if args.bootstrap_sql_dir:
            execute_sql_files(connection, Path(args.bootstrap_sql_dir))

        raw_dir = Path(args.raw_dir)
        batch_context = BatchRunContext(
            batch_id=batch_id,
            batch_date=args.batch_date,
            run_id=args.run_id,
            dag_id=args.dag_id,
        )
        iam_role_arn = required_env("REDSHIFT_COPY_IAM_ROLE_ARN")
        try:
            load_all(
                connection=connection,
                specs=load_specs(Path(args.profile)),
                batch_date=args.batch_date,
                batch_id=batch_id,
                run_id=args.run_id,
                bucket=args.s3_bucket,
                prefix=args.s3_prefix,
                aws_region=args.aws_region,
                iam_role_arn=iam_role_arn,
                dead_letter_entries=load_dead_letter_manifest_entries(raw_dir),
            )
            if not args.disable_batch_control:
                mark_batch_status(
                    connection,
                    batch_context,
                    "RAW_LOADED",
                    raw_dir=raw_dir,
                )
        except Exception as exc:
            if not args.disable_batch_control:
                mark_batch_status(
                    connection,
                    batch_context,
                    "FAILED",
                    raw_dir=raw_dir,
                    error_message=str(exc),
                )
            raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
