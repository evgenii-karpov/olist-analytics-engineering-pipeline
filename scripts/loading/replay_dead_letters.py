"""Replay corrected dead-letter rows into PostgreSQL raw tables."""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TextIO

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection

from scripts.ingestion.correction_specs import CORRECTION_FEEDS
from scripts.ingestion.raw_files import (
    METADATA_COLUMNS,
    load_source_entities,
    utc_now_string,
)
from scripts.ingestion.record_validation import validate_row
from scripts.loading.load_raw_to_postgres import RAW_SCHEMA, execute_sql_files, utc_now


@dataclass(frozen=True)
class ReplaySpec:
    entity_name: str
    columns: list[str]
    column_types: dict[str, str]


@dataclass(frozen=True)
class ReplayPayload:
    batch_id: str
    replay_source_file: str
    csv_buffer: io.StringIO
    row_count: int


def postgres_connection(args: argparse.Namespace) -> PgConnection:
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
    )


def load_replay_specs(profile_path: Path) -> dict[str, ReplaySpec]:
    source_specs = {
        entity.entity_name: ReplaySpec(
            entity_name=entity.entity_name,
            columns=entity.columns,
            column_types=entity.column_types,
        )
        for entity in load_source_entities(profile_path)
    }
    correction_specs = {
        feed.entity_name: ReplaySpec(
            entity_name=feed.entity_name,
            columns=feed.headers,
            column_types=feed.column_types,
        )
        for feed in CORRECTION_FEEDS
    }
    return {**source_specs, **correction_specs}


def default_replay_id(dead_letter_file: Path) -> str:
    name = dead_letter_file.name
    if name.endswith(".csv.gz"):
        name = name.removesuffix(".csv.gz")
    else:
        name = dead_letter_file.stem
    return f"{name}_replay"


def replay_source_file_for(replay_id: str) -> str:
    return f"dead_letter_replay:{replay_id}"[:512]


@contextmanager
def open_dead_letter_file(path: Path) -> Iterator[TextIO]:
    if path.name.endswith(".gz"):
        with gzip.open(path, mode="rt", encoding="utf-8", newline="") as file:
            yield file
    else:
        with path.open(mode="rt", encoding="utf-8", newline="") as file:
            yield file


def read_dead_letter_rows(dead_letter_file: Path) -> list[dict[str, str]]:
    with open_dead_letter_file(dead_letter_file) as input_file:
        reader = csv.DictReader(input_file)
        if reader.fieldnames is None:
            raise ValueError(f"{dead_letter_file} has no header row")
        return list(reader)


def resolve_batch_id(rows: list[dict[str, str]], requested_batch_id: str | None) -> str:
    if requested_batch_id:
        return requested_batch_id

    batch_ids = {
        batch_id for row in rows if (batch_id := row.get("_batch_id")) is not None
    }
    if len(batch_ids) != 1:
        raise ValueError(
            "Dead-letter file must contain exactly one _batch_id when --batch-id is omitted"
        )
    return batch_ids.pop()


def build_replay_payload(
    spec: ReplaySpec,
    rows: list[dict[str, str]],
    batch_id: str,
    replay_id: str,
    source_system: str,
) -> ReplayPayload:
    replay_source_file = replay_source_file_for(replay_id)
    loaded_at = utc_now_string()
    fieldnames = [*spec.columns, *METADATA_COLUMNS]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    validation_failures = []
    for index, row in enumerate(rows, start=1):
        replay_row = {column: row.get(column, "") for column in spec.columns}
        validation_errors = validate_row(replay_row, spec.column_types)
        if validation_errors:
            source_row_number = row.get("_source_row_number", str(index))
            validation_failures.append(
                f"source row {source_row_number}: {'; '.join(validation_errors)}"
            )
            continue

        replay_row["_batch_id"] = batch_id
        replay_row["_loaded_at"] = loaded_at
        replay_row["_source_file"] = replay_source_file
        replay_row["_source_system"] = source_system
        writer.writerow(replay_row)

    if validation_failures:
        joined_failures = "\n".join(
            f"- {failure}" for failure in validation_failures[:20]
        )
        raise ValueError(
            "Corrected dead-letter file still contains invalid rows:\n"
            f"{joined_failures}"
        )

    output.seek(0)
    return ReplayPayload(
        batch_id=batch_id,
        replay_source_file=replay_source_file,
        csv_buffer=output,
        row_count=len(rows),
    )


def delete_previous_replay(
    connection: PgConnection,
    spec: ReplaySpec,
    payload: ReplayPayload,
    source_system: str,
) -> None:
    delete_statement = sql.SQL(
        """
        delete from {}.{}
        where _batch_id = %s
          and _source_file = %s
          and _source_system = %s;
        """
    ).format(sql.Identifier(RAW_SCHEMA), sql.Identifier(spec.entity_name))
    with connection.cursor() as cursor:
        cursor.execute(
            delete_statement,
            (payload.batch_id, payload.replay_source_file, source_system),
        )


def copy_replay_rows(
    connection: PgConnection,
    spec: ReplaySpec,
    payload: ReplayPayload,
) -> None:
    copy_statement = sql.SQL(
        "copy {}.{} from stdin with (format csv, header true)"
    ).format(sql.Identifier(RAW_SCHEMA), sql.Identifier(spec.entity_name))
    with connection.cursor() as cursor:
        cursor.copy_expert(copy_statement.as_string(connection), payload.csv_buffer)


def delete_replay_audit(
    connection: PgConnection,
    replay_id: str,
    entity_name: str,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            delete from audit.dead_letter_replays
            where dead_letter_replay_id = %s
              and entity_name = %s;
            """,
            (replay_id, entity_name),
        )


def record_replay_success(
    connection: PgConnection,
    replay_id: str,
    spec: ReplaySpec,
    payload: ReplayPayload,
    dead_letter_file: Path,
    started_at: datetime,
) -> None:
    delete_replay_audit(connection, replay_id, spec.entity_name)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            insert into audit.dead_letter_replays (
                dead_letter_replay_id,
                batch_id,
                entity_name,
                dead_letter_uri,
                target_table,
                replay_source_file,
                status,
                rows_replayed,
                started_at,
                finished_at,
                error_message
            )
            values (%s, %s, %s, %s, %s, %s, 'SUCCESS', %s, %s, current_timestamp, null);
            """,
            (
                replay_id,
                payload.batch_id,
                spec.entity_name,
                dead_letter_file.resolve().as_uri(),
                f"{RAW_SCHEMA}.{spec.entity_name}",
                payload.replay_source_file,
                payload.row_count,
                started_at,
            ),
        )


def record_replay_failure(
    connection: PgConnection,
    replay_id: str,
    spec: ReplaySpec,
    batch_id: str,
    replay_source_file: str,
    dead_letter_file: Path,
    started_at: datetime,
    error: Exception,
) -> None:
    delete_replay_audit(connection, replay_id, spec.entity_name)
    with connection.cursor() as cursor:
        cursor.execute(
            """
            insert into audit.dead_letter_replays (
                dead_letter_replay_id,
                batch_id,
                entity_name,
                dead_letter_uri,
                target_table,
                replay_source_file,
                status,
                rows_replayed,
                started_at,
                finished_at,
                error_message
            )
            values (%s, %s, %s, %s, %s, %s, 'FAILED', 0, %s, current_timestamp, %s);
            """,
            (
                replay_id,
                batch_id,
                spec.entity_name,
                dead_letter_file.resolve().as_uri(),
                f"{RAW_SCHEMA}.{spec.entity_name}",
                replay_source_file,
                started_at,
                str(error)[:65535],
            ),
        )
    connection.commit()


def replay_dead_letters(
    connection: PgConnection,
    spec: ReplaySpec,
    payload: ReplayPayload,
    dead_letter_file: Path,
    replay_id: str,
    source_system: str,
) -> None:
    started_at = utc_now()
    try:
        delete_previous_replay(connection, spec, payload, source_system)
        copy_replay_rows(connection, spec, payload)
        record_replay_success(
            connection,
            replay_id,
            spec,
            payload,
            dead_letter_file,
            started_at,
        )
        connection.commit()
    except Exception as exc:
        connection.rollback()
        record_replay_failure(
            connection,
            replay_id,
            spec,
            payload.batch_id,
            payload.replay_source_file,
            dead_letter_file,
            started_at,
            exc,
        )
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dead-letter-file", required=True)
    parser.add_argument("--entity", required=True)
    parser.add_argument("--profile", default="docs/source_profile.json")
    parser.add_argument("--batch-id")
    parser.add_argument("--replay-id")
    parser.add_argument("--source-system", default="olist_dead_letter_replay")
    parser.add_argument("--bootstrap-sql-dir")
    parser.add_argument("--host", default=os.environ.get("POSTGRES_HOST", "localhost"))
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("POSTGRES_PORT", "5432")),
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
    dead_letter_file = Path(args.dead_letter_file)
    specs = load_replay_specs(Path(args.profile))
    if args.entity not in specs:
        raise ValueError(f"Unknown replay entity: {args.entity}")

    rows = read_dead_letter_rows(dead_letter_file)
    if not rows:
        raise ValueError(f"No rows found in {dead_letter_file}")

    batch_id = resolve_batch_id(rows, args.batch_id)
    replay_id = args.replay_id or default_replay_id(dead_letter_file)
    spec = specs[args.entity]
    payload = build_replay_payload(
        spec=spec,
        rows=rows,
        batch_id=batch_id,
        replay_id=replay_id,
        source_system=args.source_system,
    )

    connection = postgres_connection(args)
    try:
        if args.bootstrap_sql_dir:
            execute_sql_files(connection, Path(args.bootstrap_sql_dir))

        replay_dead_letters(
            connection=connection,
            spec=spec,
            payload=payload,
            dead_letter_file=dead_letter_file,
            replay_id=replay_id,
            source_system=args.source_system,
        )
    finally:
        connection.close()

    print(
        f"Replayed {payload.row_count} {spec.entity_name} rows "
        f"from {dead_letter_file} as {payload.replay_source_file}"
    )


if __name__ == "__main__":
    main()
