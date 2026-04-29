"""Generate Redshift raw and audit DDL from the source profile JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

METADATA_COLUMNS = [
    ("_batch_id", "varchar(128)", "not null"),
    ("_loaded_at", "timestamp", "not null"),
    ("_source_file", "varchar(512)", "not null"),
    ("_source_system", "varchar(64)", "not null"),
]


SCHEMAS = [
    "raw_data",
    "staging",
    "intermediate",
    "snapshots",
    "core",
    "marts",
    "audit",
]


def load_profile(profile_path: Path) -> list[dict]:
    return json.loads(profile_path.read_text(encoding="utf-8"))


def render_create_schemas() -> str:
    lines = [
        "-- Create warehouse schemas for the Olist Modern Data Stack project.",
        "-- Run as a Redshift user with schema creation privileges.",
        "",
    ]
    for schema in SCHEMAS:
        lines.append(f"create schema if not exists {schema};")
    lines.append("")
    return "\n".join(lines)


def render_raw_table(entity: dict) -> str:
    table_name = entity["entity_name"]
    lines = [f"create table if not exists raw_data.{table_name} ("]
    column_lines = []

    for column in entity["columns"]:
        column_lines.append(
            f"    {column['name']} {column['redshift_raw_type']} encode zstd"
        )

    for column_name, column_type, nullability in METADATA_COLUMNS:
        column_lines.append(
            f"    {column_name} {column_type} {nullability} encode zstd"
        )

    lines.append(",\n".join(column_lines))
    lines.append(")")
    lines.append("diststyle auto")
    lines.append("sortkey(_loaded_at);")
    return "\n".join(lines)


def render_raw_tables(profile: list[dict]) -> str:
    lines = [
        "-- Raw Olist source tables.",
        "-- Generated from docs/source_profile.json.",
        "-- Raw tables are append-only and include ingestion metadata columns.",
        "",
    ]

    for entity in profile:
        lines.append(render_raw_table(entity))
        lines.append("")

    return "\n".join(lines)


def render_audit_tables() -> str:
    return """-- Audit tables for pipeline and Redshift COPY observability.

create table if not exists audit.load_runs (
    load_run_id varchar(128) not null encode zstd,
    batch_id varchar(128) not null encode zstd,
    entity_name varchar(128) not null encode zstd,
    source_uri varchar(1024) encode zstd,
    target_table varchar(256) not null encode zstd,
    status varchar(32) not null encode zstd,
    rows_loaded bigint encode az64,
    started_at timestamp not null encode az64,
    finished_at timestamp encode az64,
    error_message varchar(65535) encode zstd
)
diststyle auto
sortkey(started_at);

create table if not exists audit.dbt_runs (
    dbt_run_id varchar(128) not null encode zstd,
    batch_id varchar(128) not null encode zstd,
    command varchar(1024) not null encode zstd,
    status varchar(32) not null encode zstd,
    started_at timestamp not null encode az64,
    finished_at timestamp encode az64,
    error_message varchar(65535) encode zstd
)
diststyle auto
sortkey(started_at);
"""


def render_copy_template(profile: list[dict]) -> str:
    lines = [
        "-- Redshift COPY template for raw Olist tables.",
        "-- Replace placeholders before running:",
        "--   <bucket>",
        "--   <prefix>",
        "--   <batch_date>",
        "--   <run_id>",
        "--   <redshift_iam_role_arn>",
        "",
        "-- Example S3 path:",
        "-- s3://<bucket>/<prefix>/raw/orders/batch_date=<batch_date>/run_id=<run_id>/orders.csv.gz",
        "",
    ]

    for entity in profile:
        entity_name = entity["entity_name"]
        source_path = (
            f"s3://<bucket>/<prefix>/raw/{entity_name}/"
            f"batch_date=<batch_date>/run_id=<run_id>/"
        )
        lines.extend(
            [
                f"copy raw_data.{entity_name}",
                f"from '{source_path}'",
                "iam_role '<redshift_iam_role_arn>'",
                "csv",
                "gzip",
                "ignoreheader 1",
                "timeformat 'auto'",
                "dateformat 'auto'",
                "emptyasnull",
                "blanksasnull",
                "acceptinvchars",
                "region '<aws_region>';",
                "",
            ]
        )

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="docs/source_profile.json")
    parser.add_argument(
        "--schemas-output", default="infra/redshift/001_create_schemas.sql"
    )
    parser.add_argument(
        "--raw-output", default="infra/redshift/002_create_raw_tables.sql"
    )
    parser.add_argument(
        "--audit-output", default="infra/redshift/003_create_audit_tables.sql"
    )
    parser.add_argument(
        "--copy-output", default="infra/redshift/004_copy_raw_tables_template.sql"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    profile = load_profile(Path(args.profile))

    outputs = {
        Path(args.schemas_output): render_create_schemas(),
        Path(args.raw_output): render_raw_tables(profile),
        Path(args.audit_output): render_audit_tables(),
        Path(args.copy_output): render_copy_template(profile),
    }

    for output_path, content in outputs.items():
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
        print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
