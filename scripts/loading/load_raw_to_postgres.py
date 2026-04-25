"""Load S3-shaped local raw files into PostgreSQL raw tables."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection

from scripts.ingestion.correction_specs import CORRECTION_FEEDS
from scripts.ingestion.raw_files import load_source_entities, raw_file_path
from scripts.orchestration.batch_control import BatchRunContext, mark_batch_status


@dataclass(frozen=True)
class RawLoadSpec:
    entity_name: str
    file_name: str


@dataclass(frozen=True)
class DeadLetterManifestEntry:
    entity_name: str
    source_uri: str | None
    dead_letter_uri: str | None
    total_rows: int
    valid_rows: int
    failed_rows: int
    threshold_max_rows: int
    threshold_max_rate: float
    reason_summary: str


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0, tzinfo=None)


def postgres_connection(args: argparse.Namespace) -> PgConnection:
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
    )


def load_specs(profile_path: Path) -> list[RawLoadSpec]:
    source_specs = [
        RawLoadSpec(
            entity_name=entity.entity_name, file_name=f"{entity.entity_name}.csv.gz"
        )
        for entity in load_source_entities(profile_path)
    ]
    correction_specs = [
        RawLoadSpec(entity_name=feed.entity_name, file_name=feed.file_name)
        for feed in CORRECTION_FEEDS
    ]
    return [*source_specs, *correction_specs]


def load_dead_letter_manifest_entries(
    raw_dir: Path,
) -> dict[str, DeadLetterManifestEntry]:
    manifest_entries: dict[str, DeadLetterManifestEntry] = {}

    for manifest_name in ("manifest.json", "correction_manifest.json"):
        manifest_path = raw_dir / manifest_name
        if not manifest_path.exists():
            continue

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        threshold = manifest.get("dead_letter_threshold") or {}

        for file_entry in manifest.get("files", []):
            entity_name = file_entry["entity_name"]
            dead_letter = file_entry.get("dead_letter") or {}
            reason_counts = dead_letter.get("reason_counts") or {}
            manifest_entries[entity_name] = DeadLetterManifestEntry(
                entity_name=entity_name,
                source_uri=file_entry.get("local_uri") or file_entry.get("s3_uri"),
                dead_letter_uri=dead_letter.get("local_uri")
                or dead_letter.get("s3_uri"),
                total_rows=int(file_entry.get("total_row_count") or 0),
                valid_rows=int(
                    file_entry.get("valid_row_count")
                    if file_entry.get("valid_row_count") is not None
                    else file_entry.get("row_count") or 0
                ),
                failed_rows=int(file_entry.get("dead_letter_row_count") or 0),
                threshold_max_rows=int(threshold.get("max_rows") or 0),
                threshold_max_rate=float(threshold.get("max_rate") or 0),
                reason_summary=json.dumps(reason_counts, sort_keys=True),
            )

    return manifest_entries


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


def copy_file_to_raw_table(
    connection: PgConnection,
    spec: RawLoadSpec,
    source_path: Path,
) -> None:
    copy_statement = sql.SQL(
        "copy {}.{} from stdin with (format csv, header true)"
    ).format(sql.Identifier("raw"), sql.Identifier(spec.entity_name))

    with (
        gzip.open(source_path, mode="rt", encoding="utf-8", newline="") as raw_file,
        connection.cursor() as cursor,
    ):
        cursor.copy_expert(copy_statement.as_string(connection), raw_file)


def record_success(
    connection: PgConnection,
    spec: RawLoadSpec,
    batch_id: str,
    run_id: str,
    source_path: Path,
    started_at: datetime,
) -> None:
    with connection.cursor() as cursor:
        count_statement = sql.SQL(
            "select count(*) from {}.{} where _batch_id = %s"
        ).format(sql.Identifier("raw"), sql.Identifier(spec.entity_name))
        cursor.execute(count_statement, (batch_id,))
        rows_loaded = cursor.fetchone()[0]
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
                source_path.resolve().as_uri(),
                f"raw.{spec.entity_name}",
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
    source_path: Path,
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
                source_path.resolve().as_uri()
                if source_path.exists()
                else str(source_path),
                f"raw.{spec.entity_name}",
                started_at,
                str(error)[:65535],
            ),
        )
    connection.commit()


def load_one_spec(
    connection: PgConnection,
    spec: RawLoadSpec,
    raw_dir: Path,
    batch_date: str,
    batch_id: str,
    run_id: str,
    dead_letter_entry: DeadLetterManifestEntry | None,
) -> None:
    source_path = raw_file_path(
        raw_dir, spec.entity_name, batch_date, run_id, spec.file_name
    )
    started_at = utc_now()
    try:
        with connection.cursor() as cursor:
            delete_statement = sql.SQL("delete from {}.{} where _batch_id = %s").format(
                sql.Identifier("raw"), sql.Identifier(spec.entity_name)
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

        if not source_path.exists():
            raise FileNotFoundError(f"Missing prepared raw file: {source_path}")

        copy_file_to_raw_table(connection, spec, source_path)
        record_success(connection, spec, batch_id, run_id, source_path, started_at)
        connection.commit()
        print(f"Loaded {spec.entity_name} from {source_path}")
    except Exception as exc:
        connection.rollback()
        record_failure(connection, spec, batch_id, run_id, source_path, started_at, exc)
        raise


def load_all(
    connection: PgConnection,
    specs: Iterable[RawLoadSpec],
    raw_dir: Path,
    batch_date: str,
    batch_id: str,
    run_id: str,
    dead_letter_entries: dict[str, DeadLetterManifestEntry],
) -> None:
    for spec in specs:
        load_one_spec(
            connection,
            spec,
            raw_dir,
            batch_date,
            batch_id,
            run_id,
            dead_letter_entries.get(spec.entity_name),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default="data/raw/olist")
    parser.add_argument("--profile", default="docs/source_profile.json")
    parser.add_argument("--bootstrap-sql-dir")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--batch-id")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dag-id")
    parser.add_argument("--disable-batch-control", action="store_true")
    parser.add_argument("--host", default=os.environ.get("POSTGRES_HOST", "localhost"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("POSTGRES_PORT", "5432"))
    )
    parser.add_argument(
        "--database", default=os.environ.get("POSTGRES_DB", "olist_analytics")
    )
    parser.add_argument("--user", default=os.environ.get("POSTGRES_USER", "olist"))
    parser.add_argument(
        "--password", default=os.environ.get("POSTGRES_PASSWORD", "olist")
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    batch_id = args.batch_id or args.batch_date
    connection = postgres_connection(args)
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
        try:
            load_all(
                connection=connection,
                specs=load_specs(Path(args.profile)),
                raw_dir=raw_dir,
                batch_date=args.batch_date,
                batch_id=batch_id,
                run_id=args.run_id,
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
