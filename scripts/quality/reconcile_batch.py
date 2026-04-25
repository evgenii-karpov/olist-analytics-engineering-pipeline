"""Reconcile prepared raw files, dead letters, replays, and raw table counts."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection

from scripts.loading.load_raw_to_postgres import (
    RawLoadSpec,
    execute_sql_files,
    load_dead_letter_manifest_entries,
    load_specs,
)
from scripts.orchestration.batch_control import BatchRunContext, mark_batch_status


@dataclass(frozen=True)
class ReconciliationInput:
    entity_name: str
    source_uri: str | None
    expected_source_rows: int | None
    prepared_total_rows: int | None
    prepared_valid_rows: int | None
    dead_letter_rows: int | None
    replayed_rows: int
    raw_loaded_rows: int


@dataclass(frozen=True)
class ReconciliationResult:
    entity_name: str
    source_uri: str | None
    expected_source_rows: int | None
    prepared_total_rows: int | None
    prepared_valid_rows: int | None
    dead_letter_rows: int | None
    replayed_rows: int
    expected_loaded_rows: int | None
    raw_loaded_rows: int
    source_to_prepared_delta: int | None
    prepared_to_loaded_delta: int | None
    status: str
    failed_checks: str | None


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


def load_expected_source_rows(profile_path: Path) -> dict[str, int]:
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    return {entity["entity_name"]: int(entity["row_count"]) for entity in profile}


def count_raw_rows(
    connection: PgConnection,
    spec: RawLoadSpec,
    batch_id: str,
) -> int:
    count_statement = sql.SQL("select count(*) from {}.{} where _batch_id = %s").format(
        sql.Identifier("raw"), sql.Identifier(spec.entity_name)
    )
    with connection.cursor() as cursor:
        cursor.execute(count_statement, (batch_id,))
        return int(cursor.fetchone()[0])


def count_replayed_rows(
    connection: PgConnection,
    entity_name: str,
    batch_id: str,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            select coalesce(sum(rows_replayed), 0)
            from audit.dead_letter_replays
            where batch_id = %s
              and entity_name = %s
              and status = 'SUCCESS';
            """,
            (batch_id, entity_name),
        )
        return int(cursor.fetchone()[0])


def evaluate_reconciliation(input_row: ReconciliationInput) -> ReconciliationResult:
    failed_checks = []

    if input_row.expected_source_rows is None:
        failed_checks.append("missing_expected_source_rows")
        source_to_prepared_delta = None
    elif input_row.prepared_total_rows is None:
        failed_checks.append("missing_manifest_entry")
        source_to_prepared_delta = None
    else:
        source_to_prepared_delta = (
            input_row.prepared_total_rows - input_row.expected_source_rows
        )
        if source_to_prepared_delta != 0:
            failed_checks.append("source_to_prepared_count_mismatch")

    if input_row.prepared_valid_rows is None:
        expected_loaded_rows = None
        prepared_to_loaded_delta = None
        if "missing_manifest_entry" not in failed_checks:
            failed_checks.append("missing_manifest_entry")
    else:
        expected_loaded_rows = input_row.prepared_valid_rows + input_row.replayed_rows
        prepared_to_loaded_delta = input_row.raw_loaded_rows - expected_loaded_rows
        if prepared_to_loaded_delta != 0:
            failed_checks.append("prepared_to_loaded_count_mismatch")

    if (
        input_row.prepared_total_rows is not None
        and input_row.prepared_valid_rows is not None
        and input_row.dead_letter_rows is not None
        and input_row.prepared_total_rows
        != input_row.prepared_valid_rows + input_row.dead_letter_rows
    ):
        failed_checks.append("valid_plus_dead_letter_count_mismatch")

    return ReconciliationResult(
        entity_name=input_row.entity_name,
        source_uri=input_row.source_uri,
        expected_source_rows=input_row.expected_source_rows,
        prepared_total_rows=input_row.prepared_total_rows,
        prepared_valid_rows=input_row.prepared_valid_rows,
        dead_letter_rows=input_row.dead_letter_rows,
        replayed_rows=input_row.replayed_rows,
        expected_loaded_rows=expected_loaded_rows,
        raw_loaded_rows=input_row.raw_loaded_rows,
        source_to_prepared_delta=source_to_prepared_delta,
        prepared_to_loaded_delta=prepared_to_loaded_delta,
        status="PASS" if not failed_checks else "FAIL",
        failed_checks=";".join(failed_checks) if failed_checks else None,
    )


def build_reconciliation_results(
    specs: list[RawLoadSpec],
    expected_source_rows: dict[str, int],
    manifest_entries: dict[str, object],
    raw_loaded_rows: dict[str, int],
    replayed_rows: dict[str, int],
) -> list[ReconciliationResult]:
    results = []
    for spec in specs:
        manifest_entry = manifest_entries.get(spec.entity_name)
        prepared_total_rows = getattr(manifest_entry, "total_rows", None)
        prepared_valid_rows = getattr(manifest_entry, "valid_rows", None)
        dead_letter_rows = getattr(manifest_entry, "failed_rows", None)
        expected_rows = expected_source_rows.get(spec.entity_name, prepared_total_rows)
        results.append(
            evaluate_reconciliation(
                ReconciliationInput(
                    entity_name=spec.entity_name,
                    source_uri=getattr(manifest_entry, "source_uri", None),
                    expected_source_rows=expected_rows,
                    prepared_total_rows=prepared_total_rows,
                    prepared_valid_rows=prepared_valid_rows,
                    dead_letter_rows=dead_letter_rows,
                    replayed_rows=replayed_rows.get(spec.entity_name, 0),
                    raw_loaded_rows=raw_loaded_rows.get(spec.entity_name, 0),
                )
            )
        )
    return results


def record_reconciliation_results(
    connection: PgConnection,
    batch_id: str,
    run_id: str,
    results: list[ReconciliationResult],
) -> None:
    created_at = utc_now()
    with connection.cursor() as cursor:
        cursor.execute(
            """
            delete from audit.batch_reconciliation
            where batch_id = %s;
            """,
            (batch_id,),
        )

        for result in results:
            cursor.execute(
                """
                insert into audit.batch_reconciliation (
                    reconciliation_run_id,
                    batch_id,
                    entity_name,
                    source_uri,
                    expected_source_rows,
                    prepared_total_rows,
                    prepared_valid_rows,
                    dead_letter_rows,
                    replayed_rows,
                    expected_loaded_rows,
                    raw_loaded_rows,
                    source_to_prepared_delta,
                    prepared_to_loaded_delta,
                    status,
                    failed_checks,
                    created_at
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    run_id,
                    batch_id,
                    result.entity_name,
                    result.source_uri,
                    result.expected_source_rows,
                    result.prepared_total_rows,
                    result.prepared_valid_rows,
                    result.dead_letter_rows,
                    result.replayed_rows,
                    result.expected_loaded_rows,
                    result.raw_loaded_rows,
                    result.source_to_prepared_delta,
                    result.prepared_to_loaded_delta,
                    result.status,
                    result.failed_checks,
                    created_at,
                ),
            )
    connection.commit()


def fail_if_mismatched(results: list[ReconciliationResult]) -> None:
    failed_results = [result for result in results if result.status != "PASS"]
    if not failed_results:
        return

    summary = "\n".join(
        (
            f"- {result.entity_name}: {result.failed_checks}; "
            f"expected_loaded={result.expected_loaded_rows}, "
            f"raw_loaded={result.raw_loaded_rows}"
        )
        for result in failed_results
    )
    raise ValueError(f"Batch reconciliation failed:\n{summary}")


def reconcile_batch(
    connection: PgConnection,
    profile_path: Path,
    raw_dir: Path,
    batch_id: str,
) -> list[ReconciliationResult]:
    specs = load_specs(profile_path)
    manifest_entries = load_dead_letter_manifest_entries(raw_dir)
    expected_source_rows = load_expected_source_rows(profile_path)
    raw_counts = {
        spec.entity_name: count_raw_rows(connection, spec, batch_id) for spec in specs
    }
    replay_counts = {
        spec.entity_name: count_replayed_rows(connection, spec.entity_name, batch_id)
        for spec in specs
    }
    return build_reconciliation_results(
        specs=specs,
        expected_source_rows=expected_source_rows,
        manifest_entries=manifest_entries,
        raw_loaded_rows=raw_counts,
        replayed_rows=replay_counts,
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
    parser.add_argument("--no-fail-on-mismatch", action="store_true")
    parser.add_argument("--disable-batch-control", action="store_true")
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
    batch_id = args.batch_id or args.batch_date
    raw_dir = Path(args.raw_dir)
    connection = postgres_connection(args)
    try:
        if args.bootstrap_sql_dir:
            execute_sql_files(connection, Path(args.bootstrap_sql_dir))

        results = reconcile_batch(
            connection=connection,
            profile_path=Path(args.profile),
            raw_dir=raw_dir,
            batch_id=batch_id,
        )
        record_reconciliation_results(connection, batch_id, args.run_id, results)

        batch_context = BatchRunContext(
            batch_id=batch_id,
            batch_date=args.batch_date,
            run_id=args.run_id,
            dag_id=args.dag_id,
        )
        try:
            if not args.no_fail_on_mismatch:
                fail_if_mismatched(results)

            if not args.disable_batch_control:
                mark_batch_status(
                    connection,
                    batch_context,
                    "RAW_RECONCILED",
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

    passed = sum(1 for result in results if result.status == "PASS")
    print(f"Reconciled batch {batch_id}: {passed}/{len(results)} entities passed")


if __name__ == "__main__":
    main()
