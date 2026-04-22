"""Prepare Olist raw files in the local S3-shaped raw zone."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.ingestion.local_storage import render_manifest
from scripts.ingestion.raw_files import prepare_entities


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", default="olist.zip")
    parser.add_argument("--profile", default="docs/source_profile.json")
    parser.add_argument("--output-dir", default="data/raw/olist")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--no-clean", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    prepared_files = prepare_entities(
        archive_path=Path(args.archive),
        profile_path=Path(args.profile),
        output_dir=Path(args.output_dir),
        batch_date=args.batch_date,
        run_id=args.run_id,
        clean=not args.no_clean,
    )

    manifest_path = render_manifest(
        prepared_files,
        Path(args.output_dir),
        manifest_name="manifest.json",
        storage="local",
    )
    print(f"Wrote {manifest_path}")

    for prepared_file in prepared_files:
        print(
            f"Prepared {prepared_file.entity_name}: "
            f"{prepared_file.row_count} rows -> {prepared_file.local_path}"
        )


if __name__ == "__main__":
    main()
