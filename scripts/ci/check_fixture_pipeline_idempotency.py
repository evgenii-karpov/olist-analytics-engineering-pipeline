"""Run the small fixture twice and assert the replay is idempotent."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ci.run_fixture_pipeline import (
    DEFAULT_ARCHIVE,
    DEFAULT_FIXTURE_BATCH_DATE,
    DEFAULT_PROFILE,
    DEFAULT_RAW_DIR,
    pipeline_env,
    run_pipeline,
)


@dataclass(frozen=True)
class RelationFingerprint:
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
    parser.add_argument("--initial-run-id", default="ci_fixture_initial")
    parser.add_argument("--replay-run-id", default="ci_fixture_replay")
    parser.add_argument("--dag-id", default="github_actions_fixture")
    parser.add_argument("--lookback-days", type=int, default=3)
    parser.add_argument("--dead-letter-max-rows", type=int, default=0)
    parser.add_argument("--dead-letter-max-rate", type=float, default=0)
    parser.add_argument("--dbt-threads", type=int, default=1)
    parser.add_argument("--dbt-bin")
    return parser.parse_args()


def pipeline_args(
    args: argparse.Namespace,
    *,
    run_id: str,
    reset_warehouse: bool,
    full_refresh: bool,
) -> argparse.Namespace:
    return argparse.Namespace(
        archive=args.archive,
        profile=args.profile,
        raw_dir=args.raw_dir,
        batch_date=args.batch_date,
        batch_id=args.batch_id,
        run_id=run_id,
        dag_id=args.dag_id,
        lookback_days=args.lookback_days,
        dead_letter_max_rows=args.dead_letter_max_rows,
        dead_letter_max_rate=args.dead_letter_max_rate,
        dbt_threads=args.dbt_threads,
        dbt_bin=args.dbt_bin,
        skip_dbt=False,
        reset_warehouse=reset_warehouse,
        full_refresh=full_refresh,
    )


def postgres_connection(env: dict[str, str]) -> PgConnection:
    return psycopg2.connect(
        host=env["POSTGRES_HOST"],
        port=int(env["POSTGRES_PORT"]),
        dbname=env["POSTGRES_DB"],
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
    )


def fingerprint_expression(columns: Sequence[str]) -> sql.SQL:
    values = [
        sql.SQL("coalesce({}::text, '<NULL>')").format(sql.Identifier(column))
        for column in columns
    ]
    return sql.SQL("concat_ws('|', {})").format(sql.SQL(", ").join(values))


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
        row_count, checksum = cursor.fetchone()
    return RelationFingerprint(row_count=int(row_count), checksum=str(checksum))


def capture_fingerprints(connection: PgConnection) -> dict[str, RelationFingerprint]:
    return {
        relation_name: relation_fingerprint(connection, relation_name, columns)
        for relation_name, columns in FINGERPRINT_COLUMNS.items()
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
        missing_rows, unexpected_rows = cursor.fetchone()

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
            orphan_count = int(cursor.fetchone()[0])
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


def main() -> None:
    args = parse_args()
    env = pipeline_env()

    print("Running initial fixture pipeline with warehouse reset", flush=True)
    run_pipeline(
        pipeline_args(
            args,
            run_id=args.initial_run_id,
            reset_warehouse=True,
            full_refresh=True,
        ),
        env,
    )
    with postgres_connection(env) as connection:
        assert_output_contracts(connection)
        initial_fingerprints = capture_fingerprints(connection)
    print_fingerprints("Initial", initial_fingerprints)

    print("Replaying fixture pipeline incrementally", flush=True)
    run_pipeline(
        pipeline_args(
            args,
            run_id=args.replay_run_id,
            reset_warehouse=False,
            full_refresh=False,
        ),
        env,
    )
    with postgres_connection(env) as connection:
        assert_output_contracts(connection)
        replay_fingerprints = capture_fingerprints(connection)
    print_fingerprints("Replay", replay_fingerprints)

    assert_replay_matches_initial(initial_fingerprints, replay_fingerprints)
    print("Fixture replay is idempotent", flush=True)


if __name__ == "__main__":
    main()
