"""Prepare Olist raw files and optionally upload them to S3.

This is the AWS-compatible wrapper around the shared raw file preparation
helpers. The local-first pipeline uses `prepare_olist_raw_files.py`; this script
keeps the original S3 entrypoint available for the Redshift design.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ingestion.local_storage import render_manifest
from scripts.ingestion.raw_files import prepare_entities
from scripts.ingestion.s3_storage import upload_files_to_s3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", default="olist.zip")
    parser.add_argument("--profile", default="docs/source_profile.json")
    parser.add_argument("--output-dir", default="data/prepared")
    parser.add_argument("--s3-prefix", default="olist")
    parser.add_argument("--s3-bucket")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--batch-id")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--no-clean", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.upload and not args.s3_bucket:
        raise ValueError("--s3-bucket is required when --upload is set")

    batch_id = args.batch_id or args.run_id
    prepared_files = prepare_entities(
        archive_path=Path(args.archive),
        profile_path=Path(args.profile),
        output_dir=Path(args.output_dir),
        batch_date=args.batch_date,
        batch_id=batch_id,
        run_id=args.run_id,
        clean=not args.no_clean,
    )

    manifest_path = render_manifest(
        prepared_files,
        Path(args.output_dir),
        manifest_name="manifest.json",
        storage="s3" if args.upload else "local-prepared",
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
    )
    print(f"Wrote {manifest_path}")

    for prepared_file in prepared_files:
        print(
            f"Prepared {prepared_file.entity_name}: "
            f"{prepared_file.row_count} rows -> {prepared_file.local_path}"
        )

    if args.upload:
        upload_files_to_s3(args.s3_bucket, args.s3_prefix, prepared_files)


if __name__ == "__main__":
    main()
