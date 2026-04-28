"""Trigger the local Airflow DAG twice on the small fixture and compare outputs."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection
from psycopg2.extensions import cursor as PgCursor

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_ARCHIVE = (
    PROJECT_ROOT / "tests" / "fixtures" / "olist_small" / "olist_small.zip"
)
DEFAULT_PROFILE = (
    PROJECT_ROOT / "tests" / "fixtures" / "olist_small" / "source_profile_small.json"
)
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "ci" / "raw" / "olist_small"
DEFAULT_FIXTURE_BATCH_DATE = "2018-09-01"
POSTGRES_SQL_DIR = PROJECT_ROOT / "infra" / "postgres"
RESET_SCHEMAS = (
    "raw",
    "audit",
    "staging",
    "intermediate",
    "snapshots",
    "core",
    "marts",
)
TERMINAL_DAG_STATES = {"success", "failed"}
VOLATILE_RAW_COLUMNS = {"_loaded_at"}


@dataclass(frozen=True)
class RelationFingerprint:
    row_count: int
    checksum: str


@dataclass(frozen=True)
class RawFileFingerprint:
    row_count: int
    checksum: str


FINGERPRINT_COLUMNS = {
    "core.dim_customer_scd2": [
        "customer_key",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "latest_correction_effective_at",
        "latest_change_reason",
        "valid_from",
        "valid_to",
        "is_current",
    ],
    "core.dim_product_scd2": [
        "product_key",
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "latest_correction_effective_at",
        "latest_change_reason",
        "valid_from",
        "valid_to",
        "is_current",
    ],
    "core.dim_seller": [
        "seller_key",
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ],
    "core.dim_date": [
        "date_key",
        "date_day",
    ],
    "core.dim_order_status": [
        "order_status_key",
        "order_status",
    ],
    "core.fact_order_items": [
        "order_item_key",
        "order_id",
        "order_item_id",
        "customer_key",
        "product_key",
        "seller_key",
        "order_status_key",
        "order_purchase_date_key",
        "order_approved_date_key",
        "order_delivered_customer_date_key",
        "order_estimated_delivery_date_key",
        "customer_id",
        "customer_unique_id",
        "product_id",
        "seller_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "shipping_limit_date",
        "price",
        "freight_value",
        "gross_item_amount",
        "allocated_payment_value",
        "delivery_days",
        "delivery_delay_days",
        "is_delivered_late",
    ],
    "marts.mart_daily_revenue": [
        "order_purchase_date",
        "gross_revenue",
        "product_revenue",
        "freight_revenue",
        "allocated_payment_revenue",
        "orders_count",
        "customers_count",
        "items_count",
        "late_deliveries_count",
        "average_order_value",
        "average_paid_order_value",
    ],
    "marts.mart_monthly_arpu": [
        "order_month",
        "active_customers",
        "orders_count",
        "total_revenue",
        "arpu",
        "orders_per_customer",
        "average_order_value",
        "repeat_customer_rate",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", default=str(DEFAULT_ARCHIVE))
    parser.add_argument("--profile", default=str(DEFAULT_PROFILE))
    parser.add_argument("--raw-dir", default=str(DEFAULT_RAW_DIR))
    parser.add_argument("--batch-date", default=DEFAULT_FIXTURE_BATCH_DATE)
    parser.add_argument("--batch-id", default=DEFAULT_FIXTURE_BATCH_DATE)
    parser.add_argument("--initial-run-id")
    parser.add_argument("--replay-run-id")
    parser.add_argument("--dag-id", default="olist_modern_data_stack_local")
    parser.add_argument("--lookback-days", type=int, default=3)
    parser.add_argument("--dead-letter-max-rows", type=int, default=0)
    parser.add_argument("--dead-letter-max-rate", type=float, default=0)
    parser.add_argument("--dbt-threads", type=int, default=1)
    parser.add_argument("--dag-registration-timeout-seconds", type=int, default=180)
    parser.add_argument("--dag-registration-poll-seconds", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--poll-seconds", type=int, default=5)
    return parser.parse_args()


def pipeline_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("POSTGRES_HOST", "postgres")
    env.setdefault("POSTGRES_PORT", "5432")
    env.setdefault("POSTGRES_DB", "olist_analytics")
    env.setdefault("POSTGRES_USER", "olist")
    env.setdefault("POSTGRES_PASSWORD", "olist")
    return env


def postgres_connection(env: dict[str, str]) -> PgConnection:
    return psycopg2.connect(
        host=env["POSTGRES_HOST"],
        port=int(env["POSTGRES_PORT"]),
        dbname=env["POSTGRES_DB"],
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
    )


def reset_warehouse(env: dict[str, str]) -> None:
    connection = postgres_connection(env)
    try:
        with connection.cursor() as cursor:
            for schema in RESET_SCHEMAS:
                cursor.execute(f"drop schema if exists {schema} cascade;")
        connection.commit()
    finally:
        connection.close()


def clean_raw_dir(raw_dir: Path) -> None:
    resolved_raw_dir = raw_dir.resolve()
    project_root = PROJECT_ROOT.resolve()
    if not resolved_raw_dir.is_relative_to(project_root):
        raise ValueError(f"Refusing to delete raw dir outside project: {raw_dir}")
    if resolved_raw_dir.exists():
        shutil.rmtree(resolved_raw_dir)


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    print(f"+ {' '.join(command)}", flush=True)
    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        print(result.stdout, end="", flush=True)
        raise subprocess.CalledProcessError(
            result.returncode,
            command,
            output=result.stdout,
        )
    return result


def print_airflow_diagnostics() -> None:
    diagnostics = [
        ["airflow", "config", "get-value", "core", "dags_folder"],
        [
            "python",
            "-c",
            "from pathlib import Path; print(sorted(str(p) for p in Path('/opt/airflow/dags').glob('*.py')))",
        ],
        ["airflow", "dags", "list-import-errors"],
        ["airflow", "dags", "list"],
    ]
    for command in diagnostics:
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        print(f"+ {' '.join(command)}", flush=True)
        print(result.stdout, end="", flush=True)


def wait_for_dag_registration(args: argparse.Namespace) -> None:
    deadline = time.monotonic() + args.dag_registration_timeout_seconds
    last_output = ""
    reserialize = subprocess.run(
        ["airflow", "dags", "reserialize"],
        cwd=PROJECT_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if reserialize.returncode != 0:
        last_output = reserialize.stdout
        print(last_output, end="", flush=True)

    while time.monotonic() < deadline:
        result = subprocess.run(
            ["airflow", "dags", "list"],
            cwd=PROJECT_ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        last_output = result.stdout
        if result.returncode == 0 and args.dag_id in result.stdout:
            print(f"DAG {args.dag_id} is registered in Airflow", flush=True)
            return

        print(f"Waiting for DAG {args.dag_id} to be registered", flush=True)
        time.sleep(args.dag_registration_poll_seconds)

    print(last_output, end="", flush=True)
    print_airflow_diagnostics()
    raise TimeoutError(
        "Timed out after "
        f"{args.dag_registration_timeout_seconds}s waiting for DAG {args.dag_id}"
    )


def dag_conf(args: argparse.Namespace, *, full_refresh: bool) -> dict[str, Any]:
    return {
        "batch_date": args.batch_date,
        "lookback_days": args.lookback_days,
        "full_refresh": full_refresh,
        "source_archive": args.archive,
        "source_profile": args.profile,
        "raw_dir": args.raw_dir,
        "dead_letter_max_rows": args.dead_letter_max_rows,
        "dead_letter_max_rate": args.dead_letter_max_rate,
    }


def airflow_metadata_connection() -> PgConnection:
    return psycopg2.connect(
        host=os.environ.get("AIRFLOW_POSTGRES_HOST", "airflow-postgres"),
        port=int(os.environ.get("AIRFLOW_POSTGRES_PORT", "5432")),
        dbname=os.environ.get("AIRFLOW_POSTGRES_DB", "airflow"),
        user=os.environ.get("AIRFLOW_POSTGRES_USER", "airflow"),
        password=os.environ.get("AIRFLOW_POSTGRES_PASSWORD", "airflow"),
    )


def fetch_dag_run_state(dag_id: str, run_id: str) -> str | None:
    with airflow_metadata_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            select state
            from dag_run
            where dag_id = %s
              and run_id = %s;
            """,
            (dag_id, run_id),
        )
        row = cursor.fetchone()
    return None if row is None else str(row[0])


def fetch_failed_tasks(dag_id: str, run_id: str) -> list[tuple[str, str]]:
    with airflow_metadata_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            select task_id, state
            from task_instance
            where dag_id = %s
              and run_id = %s
              and state in ('failed', 'upstream_failed')
            order by task_id;
            """,
            (dag_id, run_id),
        )
        rows = cursor.fetchall()
    return [(str(task_id), str(state)) for task_id, state in rows]


def trigger_dag(args: argparse.Namespace, *, run_id: str, full_refresh: bool) -> None:
    wait_for_dag_registration(args)
    try:
        unpause = run_command(["airflow", "dags", "unpause", args.dag_id])
        print(unpause.stdout, end="", flush=True)
    except subprocess.CalledProcessError as exc:
        print(exc.stdout, end="", flush=True)
        raise

    conf = json.dumps(dag_conf(args, full_refresh=full_refresh), sort_keys=True)
    result = run_command(
        [
            "airflow",
            "dags",
            "trigger",
            args.dag_id,
            "--run-id",
            run_id,
            "--conf",
            conf,
        ]
    )
    print(result.stdout, end="", flush=True)


def wait_for_dag_success(args: argparse.Namespace, *, run_id: str) -> None:
    deadline = time.monotonic() + args.timeout_seconds
    last_state = None
    while time.monotonic() < deadline:
        state = fetch_dag_run_state(args.dag_id, run_id)
        if state != last_state:
            print(f"DAG run {run_id} state: {state}", flush=True)
            last_state = state
        if state == "success":
            return
        if state in TERMINAL_DAG_STATES:
            failed_tasks = fetch_failed_tasks(args.dag_id, run_id)
            formatted_tasks = json.dumps(failed_tasks, indent=2)
            raise AssertionError(
                f"DAG run {run_id} finished with state={state}; "
                f"failed_tasks={formatted_tasks}"
            )
        time.sleep(args.poll_seconds)

    raise TimeoutError(
        f"Timed out after {args.timeout_seconds}s waiting for DAG run {run_id}"
    )


def fingerprint_expression(columns: Sequence[str]) -> sql.Composable:
    values = [
        sql.SQL("coalesce({}::text, '<NULL>')").format(sql.Identifier(column))
        for column in columns
    ]
    return sql.SQL("concat_ws('|', {})").format(sql.SQL(", ").join(values))


def fetch_one(cursor: PgCursor) -> tuple[Any, ...]:
    row = cursor.fetchone()
    if row is None:
        raise AssertionError("Expected query to return exactly one row")
    return row


def relation_fingerprint(
    connection: PgConnection,
    relation_name: str,
    columns: Sequence[str],
) -> RelationFingerprint:
    schema_name, table_name = relation_name.split(".", maxsplit=1)
    query = sql.SQL(
        """
        with row_fingerprints as (
            select md5({fingerprint_expression}) as row_fingerprint
            from {schema_name}.{table_name}
        )

        select
            count(*)::bigint as row_count,
            coalesce(
                md5(string_agg(row_fingerprint, '|' order by row_fingerprint)),
                md5('')
            ) as checksum
        from row_fingerprints;
        """
    ).format(
        fingerprint_expression=fingerprint_expression(columns),
        schema_name=sql.Identifier(schema_name),
        table_name=sql.Identifier(table_name),
    )
    with connection.cursor() as cursor:
        cursor.execute(query)
        row_count, checksum = fetch_one(cursor)
    return RelationFingerprint(row_count=int(row_count), checksum=str(checksum))


def capture_fingerprints(connection: PgConnection) -> dict[str, RelationFingerprint]:
    return {
        relation_name: relation_fingerprint(connection, relation_name, columns)
        for relation_name, columns in FINGERPRINT_COLUMNS.items()
    }


def normalized_raw_path(path: Path, raw_dir: Path) -> str:
    parts = path.relative_to(raw_dir).parts
    return "/".join(part for part in parts if not part.startswith("run_id="))


def raw_row_fingerprint(row: dict[str, str]) -> str:
    normalized_items = [
        (key, value)
        for key, value in sorted(row.items())
        if key not in VOLATILE_RAW_COLUMNS
    ]
    payload = json.dumps(normalized_items, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def raw_file_fingerprint(path: Path) -> RawFileFingerprint:
    row_hashes = []
    with gzip.open(path, mode="rt", encoding="utf-8", newline="") as raw_file:
        reader = csv.DictReader(raw_file)
        for row in reader:
            row_hashes.append(raw_row_fingerprint(row))
    checksum = hashlib.md5("|".join(sorted(row_hashes)).encode("utf-8")).hexdigest()
    return RawFileFingerprint(row_count=len(row_hashes), checksum=checksum)


def capture_raw_file_fingerprints(raw_dir: Path) -> dict[str, RawFileFingerprint]:
    return {
        normalized_raw_path(path, raw_dir): raw_file_fingerprint(path)
        for path in sorted(raw_dir.rglob("*.csv.gz"))
    }


def assert_fact_matches_staging(connection: PgConnection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            with expected_items as (
                select
                    md5(
                        order_items.order_id || '|'
                        || order_items.order_item_id::varchar
                    ) as order_item_key
                from staging.stg_olist__order_items as order_items
                inner join staging.stg_olist__orders as orders
                    on order_items.order_id = orders.order_id
            ),

            actual_items as (
                select order_item_key
                from core.fact_order_items
            ),

            missing_from_fact as (
                select count(*) as row_count
                from expected_items
                left join actual_items
                    on expected_items.order_item_key = actual_items.order_item_key
                where actual_items.order_item_key is null
            ),

            unexpected_fact_rows as (
                select count(*) as row_count
                from actual_items
                left join expected_items
                    on actual_items.order_item_key = expected_items.order_item_key
                where expected_items.order_item_key is null
            )

            select
                missing_from_fact.row_count,
                unexpected_fact_rows.row_count
            from missing_from_fact
            cross join unexpected_fact_rows;
            """
        )
        missing_rows, unexpected_rows = fetch_one(cursor)

    if missing_rows or unexpected_rows:
        raise AssertionError(
            "fact_order_items does not match the current staging grain: "
            f"missing_from_fact={missing_rows}, "
            f"unexpected_fact_rows={unexpected_rows}"
        )


def assert_no_orphan_fact_keys(connection: PgConnection) -> None:
    orphan_queries = {
        "customer_key": """
            select count(*)
            from core.fact_order_items as fact
            left join core.dim_customer_scd2 as dim
                on fact.customer_key = dim.customer_key
            where dim.customer_key is null
        """,
        "product_key": """
            select count(*)
            from core.fact_order_items as fact
            left join core.dim_product_scd2 as dim
                on fact.product_key = dim.product_key
            where dim.product_key is null
        """,
        "seller_key": """
            select count(*)
            from core.fact_order_items as fact
            left join core.dim_seller as dim
                on fact.seller_key = dim.seller_key
            where dim.seller_key is null
        """,
    }
    failures = {}
    with connection.cursor() as cursor:
        for key_name, query in orphan_queries.items():
            cursor.execute(query)
            orphan_count = int(fetch_one(cursor)[0])
            if orphan_count:
                failures[key_name] = orphan_count

    if failures:
        raise AssertionError(f"fact_order_items has orphan dimension keys: {failures}")


def assert_output_contracts(connection: PgConnection) -> None:
    assert_fact_matches_staging(connection)
    assert_no_orphan_fact_keys(connection)


def assert_replay_matches_initial(
    initial: dict[str, RelationFingerprint],
    replay: dict[str, RelationFingerprint],
) -> None:
    mismatches = {}
    for relation_name, initial_fingerprint in initial.items():
        replay_fingerprint = replay[relation_name]
        if initial_fingerprint != replay_fingerprint:
            mismatches[relation_name] = {
                "initial": initial_fingerprint.__dict__,
                "replay": replay_fingerprint.__dict__,
            }

    if mismatches:
        formatted = json.dumps(mismatches, indent=2, sort_keys=True)
        raise AssertionError(f"Fixture replay changed analytical outputs:\n{formatted}")


def assert_raw_files_match_initial(
    initial: dict[str, RawFileFingerprint],
    replay: dict[str, RawFileFingerprint],
) -> None:
    if initial == replay:
        return

    all_paths = sorted(set(initial) | set(replay))
    mismatches = {}
    for path in all_paths:
        if initial.get(path) != replay.get(path):
            mismatches[path] = {
                "initial": None if path not in initial else initial[path].__dict__,
                "replay": None if path not in replay else replay[path].__dict__,
            }

    formatted = json.dumps(mismatches, indent=2, sort_keys=True)
    raise AssertionError(f"Fixture replay changed raw file outputs:\n{formatted}")


def print_fingerprints(
    label: str,
    fingerprints: dict[str, RelationFingerprint],
) -> None:
    print(f"{label} analytical fingerprints:", flush=True)
    for relation_name, fingerprint in fingerprints.items():
        print(
            f"- {relation_name}: rows={fingerprint.row_count}, "
            f"checksum={fingerprint.checksum}",
            flush=True,
        )


def print_raw_fingerprints(
    label: str,
    fingerprints: dict[str, RawFileFingerprint],
) -> None:
    print(f"{label} raw file fingerprints:", flush=True)
    for path, fingerprint in fingerprints.items():
        print(
            f"- {path}: rows={fingerprint.row_count}, checksum={fingerprint.checksum}",
            flush=True,
        )


def main() -> None:
    args = parse_args()
    env = pipeline_env()
    raw_dir = Path(args.raw_dir)
    run_id_suffix = str(int(time.time()))
    args.initial_run_id = args.initial_run_id or f"ci_fixture_initial_{run_id_suffix}"
    args.replay_run_id = args.replay_run_id or f"ci_fixture_replay_{run_id_suffix}"

    print("Resetting warehouse for initial fixture DAG run", flush=True)
    clean_raw_dir(raw_dir)
    reset_warehouse(env)

    print("Triggering initial fixture DAG run", flush=True)
    trigger_dag(args, run_id=args.initial_run_id, full_refresh=True)
    wait_for_dag_success(args, run_id=args.initial_run_id)

    initial_raw_fingerprints = capture_raw_file_fingerprints(raw_dir)
    print_raw_fingerprints("Initial", initial_raw_fingerprints)
    with postgres_connection(env) as connection:
        assert_output_contracts(connection)
        initial_fingerprints = capture_fingerprints(connection)
    print_fingerprints("Initial", initial_fingerprints)

    print("Triggering replay fixture DAG run", flush=True)
    trigger_dag(args, run_id=args.replay_run_id, full_refresh=False)
    wait_for_dag_success(args, run_id=args.replay_run_id)

    replay_raw_fingerprints = capture_raw_file_fingerprints(raw_dir)
    print_raw_fingerprints("Replay", replay_raw_fingerprints)
    with postgres_connection(env) as connection:
        assert_output_contracts(connection)
        replay_fingerprints = capture_fingerprints(connection)
    print_fingerprints("Replay", replay_fingerprints)

    assert_raw_files_match_initial(initial_raw_fingerprints, replay_raw_fingerprints)
    assert_replay_matches_initial(initial_fingerprints, replay_fingerprints)
    print("Fixture replay is idempotent", flush=True)


if __name__ == "__main__":
    main()
