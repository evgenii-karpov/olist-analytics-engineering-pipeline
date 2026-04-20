"""Generate deterministic correction feeds for SCD2 simulation.

Olist is a static Kaggle dataset. These generated feeds simulate master-data
updates arriving in later batches, such as customer address changes and product
attribute corrections. The feeds are small, deterministic, and based on real
business keys from `olist.zip`.
"""

from __future__ import annotations

import argparse
import csv
import gzip
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZipFile


SOURCE_SYSTEM = "olist_corrections"
METADATA_COLUMNS = ["_batch_id", "_loaded_at", "_source_file", "_source_system"]


@dataclass(frozen=True)
class FeedSpec:
    entity_name: str
    file_name: str
    headers: list[str]


CUSTOMER_FEED = FeedSpec(
    entity_name="customer_profile_changes",
    file_name="customer_profile_changes.csv.gz",
    headers=[
        "customer_unique_id",
        "effective_at",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "change_reason",
    ],
)

PRODUCT_FEED = FeedSpec(
    entity_name="product_attribute_changes",
    file_name="product_attribute_changes.csv.gz",
    headers=[
        "product_id",
        "effective_at",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "change_reason",
    ],
)


@dataclass(frozen=True)
class PreparedCorrectionFeed:
    entity_name: str
    local_path: Path
    s3_key: str
    row_count: int


def utc_now_string() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
        if datetime.strptime(row["effective_at"], "%Y-%m-%d %H:%M:%S") <= batch_timestamp
    ]


def write_feed(
    output_dir: Path,
    feed: FeedSpec,
    rows: list[dict[str, str]],
    batch_id: str,
    loaded_at: str,
) -> Path:
    output_path = output_dir / "raw" / feed.entity_name / feed.file_name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(output_path, mode="wt", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=[*feed.headers, *METADATA_COLUMNS])
        writer.writeheader()
        for row in rows:
            row = dict(row)
            row["_batch_id"] = batch_id
            row["_loaded_at"] = loaded_at
            row["_source_file"] = feed.file_name
            row["_source_system"] = SOURCE_SYSTEM
            writer.writerow(row)

    return output_path


def s3_key_for(prefix: str, entity_name: str, batch_date: str, run_id: str, file_name: str) -> str:
    normalized_prefix = prefix.strip("/")
    return (
        f"{normalized_prefix}/raw/{entity_name}/"
        f"batch_date={batch_date}/run_id={run_id}/{file_name}"
    )


def upload_to_s3(bucket: str, prepared_feeds: list[PreparedCorrectionFeed]) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for S3 upload. Install project dependencies first.") from exc

    s3_client = boto3.client("s3")
    for prepared_feed in prepared_feeds:
        s3_client.upload_file(str(prepared_feed.local_path), bucket, prepared_feed.s3_key)
        print(f"Uploaded s3://{bucket}/{prepared_feed.s3_key}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", default="olist.zip")
    parser.add_argument("--output-dir", default="data/generated_corrections")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--s3-bucket")
    parser.add_argument("--s3-prefix", default="olist")
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--customer-count", type=int, default=20)
    parser.add_argument("--product-count", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    loaded_at = utc_now_string()

    if args.upload and not args.s3_bucket:
        raise ValueError("--s3-bucket is required when --upload is set")

    with ZipFile(args.archive) as zip_file:
        customer_rows = customer_corrections(
            read_unique_customers(zip_file, args.customer_count)
        )
        product_rows = product_corrections(read_products(zip_file, args.product_count))

    customer_rows = filter_visible_corrections(customer_rows, args.batch_date)
    product_rows = filter_visible_corrections(product_rows, args.batch_date)

    customer_path = write_feed(
        output_dir,
        CUSTOMER_FEED,
        customer_rows,
        args.run_id,
        loaded_at,
    )
    product_path = write_feed(
        output_dir,
        PRODUCT_FEED,
        product_rows,
        args.run_id,
        loaded_at,
    )

    prepared_feeds = [
        PreparedCorrectionFeed(
            entity_name=CUSTOMER_FEED.entity_name,
            local_path=customer_path,
            s3_key=s3_key_for(
                args.s3_prefix,
                CUSTOMER_FEED.entity_name,
                args.batch_date,
                args.run_id,
                CUSTOMER_FEED.file_name,
            ),
            row_count=len(customer_rows),
        ),
        PreparedCorrectionFeed(
            entity_name=PRODUCT_FEED.entity_name,
            local_path=product_path,
            s3_key=s3_key_for(
                args.s3_prefix,
                PRODUCT_FEED.entity_name,
                args.batch_date,
                args.run_id,
                PRODUCT_FEED.file_name,
            ),
            row_count=len(product_rows),
        ),
    ]

    print(f"Wrote {len(customer_rows)} customer corrections -> {customer_path}")
    print(f"Wrote {len(product_rows)} product corrections -> {product_path}")

    if args.upload:
        upload_to_s3(args.s3_bucket, prepared_feeds)


if __name__ == "__main__":
    main()
