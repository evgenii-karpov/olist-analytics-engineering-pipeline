"""Generate deterministic correction feeds for SCD2 simulation.

Olist is a static Kaggle dataset. These generated feeds simulate master-data
updates arriving in later batches, such as customer address changes and product
attribute corrections. The feeds are small, deterministic, and based on real
business keys from `olist.zip`.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ingestion.correction_specs import (
    CORRECTION_FEEDS,
    CUSTOMER_FEED,
    PRODUCT_FEED,
    FeedSpec,
)
from scripts.ingestion.local_storage import render_manifest
from scripts.ingestion.raw_files import (
    PreparedFile,
    clean_entity_run_dirs,
    utc_now_string,
    write_validated_rows,
)
from scripts.ingestion.record_validation import (
    DeadLetterThreshold,
    assert_dead_letter_thresholds,
)
from scripts.ingestion.s3_storage import upload_files_to_s3

SOURCE_SYSTEM = "olist_corrections"


def read_unique_customers(zip_file: ZipFile, limit: int) -> list[dict[str, str]]:
    with zip_file.open("olist_customers_dataset.csv") as raw_file:
        reader = csv.DictReader(line.decode("utf-8-sig") for line in raw_file)
        by_unique_id = {}
        for row in reader:
            by_unique_id.setdefault(row["customer_unique_id"], row)
    return [by_unique_id[key] for key in sorted(by_unique_id)[:limit]]


def read_products(zip_file: ZipFile, limit: int) -> list[dict[str, str]]:
    with zip_file.open("olist_products_dataset.csv") as raw_file:
        reader = csv.DictReader(line.decode("utf-8-sig") for line in raw_file)
        rows = [row for row in reader if row["product_category_name"]]
    return sorted(rows, key=lambda row: row["product_id"])[:limit]


def customer_corrections(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    cities = [
        ("01310", "sao paulo", "SP"),
        ("20040", "rio de janeiro", "RJ"),
        ("30130", "belo horizonte", "MG"),
        ("80010", "curitiba", "PR"),
        ("40020", "salvador", "BA"),
    ]
    effective_dates = [
        "2017-04-01 00:00:00",
        "2017-08-01 00:00:00",
        "2018-01-15 00:00:00",
        "2018-04-01 00:00:00",
        "2018-07-01 00:00:00",
    ]

    corrections = []
    for index, row in enumerate(rows):
        zip_code, city, state = cities[index % len(cities)]
        corrections.append(
            {
                "customer_unique_id": row["customer_unique_id"],
                "effective_at": effective_dates[index % len(effective_dates)],
                "customer_zip_code_prefix": zip_code,
                "customer_city": city,
                "customer_state": state,
                "change_reason": "simulated_address_update",
            }
        )
    return corrections


def product_corrections(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    categories = [
        "informatica_acessorios",
        "telefonia",
        "moveis_decoracao",
        "utilidades_domesticas",
        "beleza_saude",
    ]
    effective_dates = [
        "2017-05-01 00:00:00",
        "2017-09-01 00:00:00",
        "2018-02-01 00:00:00",
        "2018-05-01 00:00:00",
        "2018-08-01 00:00:00",
    ]

    corrections = []
    for index, row in enumerate(rows):
        weight = int(row["product_weight_g"] or "0") + 25
        length = int(row["product_length_cm"] or "0")
        height = int(row["product_height_cm"] or "0")
        width = int(row["product_width_cm"] or "0")
        corrections.append(
            {
                "product_id": row["product_id"],
                "effective_at": effective_dates[index % len(effective_dates)],
                "product_category_name": categories[index % len(categories)],
                "product_weight_g": str(weight),
                "product_length_cm": str(length),
                "product_height_cm": str(height),
                "product_width_cm": str(width),
                "change_reason": "simulated_product_attribute_correction",
            }
        )
    return corrections


def filter_visible_corrections(
    rows: list[dict[str, str]],
    batch_date: str,
) -> list[dict[str, str]]:
    batch_timestamp = datetime.strptime(batch_date, "%Y-%m-%d")
    return [
        row
        for row in rows
        if datetime.strptime(row["effective_at"], "%Y-%m-%d %H:%M:%S")
        <= batch_timestamp
    ]


def write_feed(
    output_dir: Path,
    feed: FeedSpec,
    rows: list[dict[str, str]],
    batch_date: str,
    batch_id: str,
    run_id: str,
    loaded_at: str,
) -> PreparedFile:
    return write_validated_rows(
        rows=enumerate(rows, start=2),
        output_dir=output_dir,
        entity_name=feed.entity_name,
        file_name=feed.file_name,
        columns=feed.headers,
        column_types=feed.column_types,
        batch_date=batch_date,
        batch_id=batch_id,
        run_id=run_id,
        loaded_at=loaded_at,
        source_file=feed.file_name,
        source_system=SOURCE_SYSTEM,
        dead_letter_stage="correction_feed_generation",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", default="olist.zip")
    parser.add_argument("--output-dir", default="data/raw/olist")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--s3-bucket")
    parser.add_argument("--s3-prefix", default="olist")
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--batch-id")
    parser.add_argument("--dead-letter-max-rows", type=int, default=10)
    parser.add_argument("--dead-letter-max-rate", type=float, default=0.001)
    parser.add_argument("--customer-count", type=int, default=20)
    parser.add_argument("--product-count", type=int, default=20)
    parser.add_argument("--no-clean", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    loaded_at = utc_now_string()

    if args.upload and not args.s3_bucket:
        raise ValueError("--s3-bucket is required when --upload is set")

    batch_id = args.batch_id or (args.run_id if args.upload else args.batch_date)
    dead_letter_threshold = DeadLetterThreshold(
        max_rows=args.dead_letter_max_rows,
        max_rate=args.dead_letter_max_rate,
    )

    if not args.no_clean:
        clean_entity_run_dirs(
            output_dir,
            (feed.entity_name for feed in CORRECTION_FEEDS),
            args.batch_date,
            args.run_id,
        )

    with ZipFile(args.archive) as zip_file:
        customer_rows = customer_corrections(
            read_unique_customers(zip_file, args.customer_count)
        )
        product_rows = product_corrections(read_products(zip_file, args.product_count))

    customer_rows = filter_visible_corrections(customer_rows, args.batch_date)
    product_rows = filter_visible_corrections(product_rows, args.batch_date)

    prepared_feeds = [
        write_feed(
            output_dir,
            CUSTOMER_FEED,
            customer_rows,
            args.batch_date,
            batch_id,
            args.run_id,
            loaded_at,
        ),
        write_feed(
            output_dir,
            PRODUCT_FEED,
            product_rows,
            args.batch_date,
            batch_id,
            args.run_id,
            loaded_at,
        ),
    ]

    manifest_path = render_manifest(
        prepared_feeds,
        output_dir,
        manifest_name="correction_manifest.json",
        storage="s3" if args.upload else "local",
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        dead_letter_threshold=dead_letter_threshold,
    )
    print(f"Wrote {manifest_path}")

    for prepared_feed in prepared_feeds:
        print(
            f"Wrote {prepared_feed.row_count} valid {prepared_feed.entity_name} rows "
            f"and {prepared_feed.dead_letter_row_count} dead-letter rows "
            f"-> {prepared_feed.local_path}"
        )

    assert_dead_letter_thresholds(prepared_feeds, dead_letter_threshold)

    if args.upload:
        upload_files_to_s3(args.s3_bucket, args.s3_prefix, prepared_feeds)


if __name__ == "__main__":
    main()
