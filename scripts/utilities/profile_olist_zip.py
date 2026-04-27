"""Profile the Olist Kaggle dataset archive and generate a source contract.

The script intentionally uses only the Python standard library so it can run in
minimal local environments before project dependencies are installed.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile

EXPECTED_FILES = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation",
}

TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)

WAREHOUSE_TYPE_OVERRIDES = {
    "customer_zip_code_prefix": "varchar(16)",
    "geolocation_zip_code_prefix": "varchar(16)",
    "seller_zip_code_prefix": "varchar(16)",
    "geolocation_lat": "decimal(18, 14)",
    "geolocation_lng": "decimal(18, 14)",
    "price": "decimal(18, 2)",
    "freight_value": "decimal(18, 2)",
    "payment_value": "decimal(18, 2)",
    "review_comment_title": "varchar(1024)",
    "review_comment_message": "varchar(65535)",
}


@dataclass
class ColumnProfile:
    name: str
    null_count: int = 0
    non_null_count: int = 0
    sample_values: list[str] = field(default_factory=list)
    inferred_type: str = "unknown"
    min_value: str | None = None
    max_value: str | None = None


@dataclass
class FileProfile:
    file_name: str
    entity_name: str
    row_count: int
    columns: list[ColumnProfile]


def is_null(value: str | None) -> bool:
    return value is None or value == ""


def try_int(value: str) -> bool:
    try:
        int(value)
        return True
    except ValueError:
        return False


def try_float(value: str) -> bool:
    try:
        parsed = float(value)
    except ValueError:
        return False
    return math.isfinite(parsed)


def try_timestamp(value: str) -> bool:
    return parse_timestamp(value) is not None


def parse_timestamp(value: str) -> datetime | None:
    for timestamp_format in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(value, timestamp_format)
        except ValueError:
            continue
    return None


def infer_type(values: Iterable[str]) -> str:
    values = [value for value in values if not is_null(value)]
    if not values:
        return "varchar"
    if all(try_int(value) for value in values):
        return "integer"
    if all(try_float(value) for value in values):
        return "decimal"
    if all(try_timestamp(value) for value in values):
        if all(len(value) == 10 for value in values):
            return "date"
        return "timestamp"
    return "varchar"


def warehouse_raw_type(column_name: str, inferred_type: str) -> str:
    if column_name in WAREHOUSE_TYPE_OVERRIDES:
        return WAREHOUSE_TYPE_OVERRIDES[column_name]

    mapping = {
        "integer": "integer",
        "decimal": "decimal(18, 6)",
        "date": "date",
        "timestamp": "timestamp",
        "varchar": "varchar(256)",
    }
    return mapping.get(inferred_type, "varchar(256)")


def profile_csv(zip_file: ZipFile, file_name: str, entity_name: str) -> FileProfile:
    with zip_file.open(file_name) as raw_file:
        text_file = (line.decode("utf-8-sig") for line in raw_file)
        reader = csv.DictReader(text_file)

        if reader.fieldnames is None:
            raise ValueError(f"{file_name} has no header row")

        column_stats = {
            column_name: ColumnProfile(name=column_name)
            for column_name in reader.fieldnames
        }
        sampled_values: dict[str, list[str]] = defaultdict(list)
        row_count = 0

        for row in reader:
            row_count += 1
            for column_name in reader.fieldnames:
                value = row.get(column_name)
                profile = column_stats[column_name]

                if is_null(value):
                    profile.null_count += 1
                    continue

                if value is None:
                    continue

                value = value.strip()
                profile.non_null_count += 1

                if (
                    len(profile.sample_values) < 3
                    and value not in profile.sample_values
                ):
                    profile.sample_values.append(value)

                if len(sampled_values[column_name]) < 1000:
                    sampled_values[column_name].append(value)

                if profile.min_value is None or value < profile.min_value:
                    profile.min_value = value
                if profile.max_value is None or value > profile.max_value:
                    profile.max_value = value

        columns = []
        for column_name in reader.fieldnames:
            profile = column_stats[column_name]
            profile.inferred_type = infer_type(sampled_values[column_name])
            columns.append(profile)

    return FileProfile(
        file_name=file_name,
        entity_name=entity_name,
        row_count=row_count,
        columns=columns,
    )


def profile_archive(archive_path: Path) -> list[FileProfile]:
    with ZipFile(archive_path) as zip_file:
        archive_names = set(zip_file.namelist())
        missing_files = sorted(set(EXPECTED_FILES) - archive_names)

        if missing_files:
            missing = ", ".join(missing_files)
            raise ValueError(f"Missing expected files in archive: {missing}")

        return [
            profile_csv(zip_file, file_name, entity_name)
            for file_name, entity_name in EXPECTED_FILES.items()
        ]


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        output.append("| " + " | ".join(row) + " |")
    return "\n".join(output)


def render_contract(profiles: list[FileProfile], archive_path: Path) -> str:
    summary_rows = [
        [
            profile.entity_name,
            f"`{profile.file_name}`",
            str(profile.row_count),
            str(len(profile.columns)),
        ]
        for profile in profiles
    ]

    lines = [
        "# Source Contract",
        "",
        "This document is generated from the local Olist dataset archive.",
        "",
        f"Source archive: `{archive_path.name}`",
        "",
        "## File Summary",
        "",
        markdown_table(
            ["Entity", "File", "Rows", "Columns"],
            summary_rows,
        ),
        "",
        "## Expected Source Files",
        "",
        "The ingestion layer should fail fast if any of these files are missing:",
        "",
    ]

    lines.extend(f"- `{file_name}`" for file_name in EXPECTED_FILES)

    lines.extend(
        [
            "",
            "## Entity Contracts",
            "",
            "The `Inferred type` column is based on sampled non-null values. The",
            "`Warehouse raw type` column is the recommended first-pass type for raw",
            "DDL. Staging models can cast to stricter business types where needed.",
            "",
        ]
    )

    for profile in profiles:
        rows = []
        for column in profile.columns:
            null_pct = (
                column.null_count / profile.row_count * 100 if profile.row_count else 0
            )
            rows.append(
                [
                    f"`{column.name}`",
                    column.inferred_type,
                    warehouse_raw_type(column.name, column.inferred_type),
                    str(column.null_count),
                    f"{null_pct:.2f}%",
                    ", ".join(f"`{value}`" for value in column.sample_values),
                ]
            )

        lines.extend(
            [
                f"### {profile.entity_name}",
                "",
                f"Source file: `{profile.file_name}`",
                "",
                f"Rows: `{profile.row_count}`",
                "",
                markdown_table(
                    [
                        "Column",
                        "Inferred type",
                        "Warehouse raw type",
                        "Nulls",
                        "Null %",
                        "Sample values",
                    ],
                    rows,
                ),
                "",
            ]
        )

    lines.extend(
        [
            "## Raw Metadata Columns",
            "",
            "Every raw table should include these ingestion metadata columns:",
            "",
            markdown_table(
                ["Column", "Recommended type", "Description"],
                [
                    ["`_batch_id`", "varchar(128)", "Deterministic batch identifier."],
                    ["`_loaded_at`", "timestamp", "Warehouse load timestamp."],
                    [
                        "`_source_file`",
                        "varchar(512)",
                        "Original raw object or source file.",
                    ],
                    [
                        "`_source_system`",
                        "varchar(64)",
                        "Source system name, initially `olist_kaggle`.",
                    ],
                ],
            ),
            "",
            "## Contract Rules",
            "",
            "- Ingestion must fail if an expected file is missing.",
            "- Ingestion must fail if a source header changes unexpectedly.",
            "- Raw data should be append-only and batch-addressable.",
            "- dbt staging models own stricter type casting and business naming.",
            "- Correction feeds for SCD2 simulation should be versioned as separate source entities.",
            "",
        ]
    )

    return "\n".join(lines)


def write_json_profile(profiles: list[FileProfile], output_path: Path) -> None:
    payload = []
    for profile in profiles:
        payload.append(
            {
                "file_name": profile.file_name,
                "entity_name": profile.entity_name,
                "row_count": profile.row_count,
                "columns": [
                    {
                        "name": column.name,
                        "null_count": column.null_count,
                        "non_null_count": column.non_null_count,
                        "inferred_type": column.inferred_type,
                        "redshift_raw_type": warehouse_raw_type(
                            column.name,
                            column.inferred_type,
                        ),
                        "sample_values": column.sample_values,
                        "min_value": column.min_value,
                        "max_value": column.max_value,
                    }
                    for column in profile.columns
                ],
            }
        )
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", default="olist.zip")
    parser.add_argument("--markdown-output", default="docs/source_contract.md")
    parser.add_argument("--json-output", default="docs/source_profile.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    archive_path = Path(args.archive)
    markdown_output_path = Path(args.markdown_output)
    json_output_path = Path(args.json_output)

    profiles = profile_archive(archive_path)

    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        render_contract(profiles, archive_path),
        encoding="utf-8",
    )

    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    write_json_profile(profiles, json_output_path)

    print(f"Profiled {len(profiles)} files from {archive_path}")
    print(f"Wrote {markdown_output_path}")
    print(f"Wrote {json_output_path}")


if __name__ == "__main__":
    main()
