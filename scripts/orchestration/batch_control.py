"""Maintain audit.batch_runs as the batch-level pipeline state machine."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2
from psycopg2.extensions import connection as PgConnection

STATUS_ORDER = {
    "STARTED": 0,
    "SOURCE_VALIDATED": 10,
    "RAW_PREPARED": 20,
    "RAW_LOADED": 30,
    "RAW_RECONCILED": 40,
    "DBT_SNAPSHOT_INPUTS_BUILT": 50,
    "DBT_SNAPSHOTTED": 60,
    "DBT_BUILT": 70,
    "TESTED": 80,
}
TERMINAL_STATUSES = {"TESTED", "FAILED"}
VALID_STATUSES = {*STATUS_ORDER, "FAILED"}


@dataclass(frozen=True)
class BatchRunContext:
    batch_id: str
    batch_date: str
    run_id: str
    dag_id: str | None = None


@dataclass(frozen=True)
class ManifestUris:
    raw_manifest_uri: str | None
    correction_manifest_uri: str | None


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0, tzinfo=None)


def warehouse_env(name: str, postgres_fallback: str, default: str) -> str:
    return os.environ.get(name, os.environ.get(postgres_fallback, default))


def warehouse_connection(args: argparse.Namespace) -> PgConnection:
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
    )


def execute_sql_files(connection: PgConnection, sql_dir: Path) -> None:
    sql_files = [
        "001_create_schemas.sql",
        "002_create_raw_tables.sql",
        "003_create_audit_tables.sql",
        "005_create_correction_tables.sql",
    ]
    with connection.cursor() as cursor:
        for file_name in sql_files:
            sql_path = sql_dir / file_name
            cursor.execute(sql_path.read_text(encoding="utf-8"))
            print(f"Executed {sql_path}")
    connection.commit()


def validate_status(status: str) -> None:
    if status not in VALID_STATUSES:
        raise ValueError(
            f"Unknown batch status {status!r}. "
            f"Expected one of: {', '.join(sorted(VALID_STATUSES))}"
        )


def validate_transition(current_status: str | None, next_status: str) -> None:
    validate_status(next_status)

    if current_status is None or next_status == "FAILED":
        return

    validate_status(current_status)
    if current_status in TERMINAL_STATUSES and current_status != next_status:
        raise ValueError(
            f"Cannot move batch from terminal status {current_status} to {next_status}"
        )

    if current_status == next_status:
        return

    if STATUS_ORDER[next_status] < STATUS_ORDER[current_status]:
        raise ValueError(
            f"Cannot move batch backwards from {current_status} to {next_status}"
        )


def manifest_uris(raw_dir: Path | None) -> ManifestUris:
    if raw_dir is None:
        return ManifestUris(raw_manifest_uri=None, correction_manifest_uri=None)

    raw_manifest = raw_dir / "manifest.json"
    correction_manifest = raw_dir / "correction_manifest.json"
    return ManifestUris(
        raw_manifest_uri=raw_manifest.resolve().as_uri()
        if raw_manifest.exists()
        else None,
        correction_manifest_uri=correction_manifest.resolve().as_uri()
        if correction_manifest.exists()
        else None,
    )


def current_batch_status(connection: PgConnection, batch_id: str) -> str | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            select status
            from audit.batch_runs
            where batch_id = %s
            order by updated_at desc
            limit 1;
            """,
            (batch_id,),
        )
        row = cursor.fetchone()
    return row[0] if row else None


def start_batch_run(
    connection: PgConnection,
    context: BatchRunContext,
    raw_dir: Path | None = None,
) -> None:
    uris = manifest_uris(raw_dir)
    now = utc_now()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            delete from audit.batch_runs
            where batch_id = %s;
            """,
            (context.batch_id,),
        )
        cursor.execute(
            """
            insert into audit.batch_runs (
                batch_id,
                batch_date,
                orchestration_run_id,
                dag_id,
                status,
                started_at,
                updated_at,
                finished_at,
                raw_manifest_uri,
                correction_manifest_uri,
                error_message
            )
            values (%s, %s, %s, %s, 'STARTED', %s, %s, null, %s, %s, null);
            """,
            (
                context.batch_id,
                context.batch_date,
                context.run_id,
                context.dag_id,
                now,
                now,
                uris.raw_manifest_uri,
                uris.correction_manifest_uri,
            ),
        )
    connection.commit()


def mark_batch_status(
    connection: PgConnection,
    context: BatchRunContext,
    status: str,
    raw_dir: Path | None = None,
    error_message: str | None = None,
) -> None:
    current_status = current_batch_status(connection, context.batch_id)
    validate_transition(current_status, status)

    uris = manifest_uris(raw_dir)
    now = utc_now()
    finished_at = now if status in TERMINAL_STATUSES else None

    with connection.cursor() as cursor:
        if current_status is None:
            cursor.execute(
                """
                insert into audit.batch_runs (
                    batch_id,
                    batch_date,
                    orchestration_run_id,
                    dag_id,
                    status,
                    started_at,
                    updated_at,
                    finished_at,
                    raw_manifest_uri,
                    correction_manifest_uri,
                    error_message
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    context.batch_id,
                    context.batch_date,
                    context.run_id,
                    context.dag_id,
                    status,
                    now,
                    now,
                    finished_at,
                    uris.raw_manifest_uri,
                    uris.correction_manifest_uri,
                    truncate_error(error_message),
                ),
            )
        else:
            cursor.execute(
                """
                update audit.batch_runs
                set orchestration_run_id = %s,
                    dag_id = coalesce(%s, dag_id),
                    status = %s,
                    updated_at = %s,
                    finished_at = %s,
                    raw_manifest_uri = coalesce(%s, raw_manifest_uri),
                    correction_manifest_uri = coalesce(%s, correction_manifest_uri),
                    error_message = %s
                where batch_id = %s;
                """,
                (
                    context.run_id,
                    context.dag_id,
                    status,
                    now,
                    finished_at,
                    uris.raw_manifest_uri,
                    uris.correction_manifest_uri,
                    truncate_error(error_message),
                    context.batch_id,
                ),
            )
    connection.commit()


def truncate_error(error_message: str | None) -> str | None:
    if error_message is None:
        return None
    return error_message[:65535]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    start_parser = subparsers.add_parser("start")
    add_common_args(start_parser)

    mark_parser = subparsers.add_parser("mark")
    add_common_args(mark_parser)
    mark_parser.add_argument("--status", required=True)
    mark_parser.add_argument("--error-message")

    fail_parser = subparsers.add_parser("fail")
    add_common_args(fail_parser)
    fail_parser.add_argument("--error-message")

    parser.add_argument(
        "--host",
        default=warehouse_env("WAREHOUSE_HOST", "POSTGRES_HOST", "localhost"),
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(warehouse_env("WAREHOUSE_PORT", "POSTGRES_PORT", "5432")),
    )
    parser.add_argument(
        "--database",
        default=warehouse_env("WAREHOUSE_DB", "POSTGRES_DB", "olist_analytics"),
    )
    parser.add_argument(
        "--user",
        default=warehouse_env("WAREHOUSE_USER", "POSTGRES_USER", "olist"),
    )
    parser.add_argument(
        "--password",
        default=warehouse_env("WAREHOUSE_PASSWORD", "POSTGRES_PASSWORD", "olist"),
    )
    return parser.parse_args()


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--batch-id")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dag-id")
    parser.add_argument("--raw-dir")
    parser.add_argument("--bootstrap-sql-dir")


def context_from_args(args: argparse.Namespace) -> BatchRunContext:
    return BatchRunContext(
        batch_id=args.batch_id or args.batch_date,
        batch_date=args.batch_date,
        run_id=args.run_id,
        dag_id=args.dag_id,
    )


def main() -> None:
    args = parse_args()
    context = context_from_args(args)
    raw_dir = Path(args.raw_dir) if args.raw_dir else None

    connection = warehouse_connection(args)
    try:
        if args.bootstrap_sql_dir:
            execute_sql_files(connection, Path(args.bootstrap_sql_dir))

        if args.command == "start":
            start_batch_run(connection, context, raw_dir=raw_dir)
            print(f"Started batch {context.batch_id} for run {context.run_id}")
        elif args.command == "mark":
            mark_batch_status(
                connection,
                context,
                args.status,
                raw_dir=raw_dir,
                error_message=args.error_message,
            )
            print(f"Marked batch {context.batch_id} as {args.status}")
        elif args.command == "fail":
            mark_batch_status(
                connection,
                context,
                "FAILED",
                raw_dir=raw_dir,
                error_message=args.error_message,
            )
            print(f"Marked batch {context.batch_id} as FAILED")
        else:
            raise ValueError(f"Unsupported command: {args.command}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
