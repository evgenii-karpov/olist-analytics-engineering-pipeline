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
from scripts.ingestion.record_validation import (
    DeadLetterThreshold,
    assert_dead_letter_thresholds,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", default="olist.zip")
    parser.add_argument("--profile", default="docs/source_profile.json")
    parser.add_argument("--output-dir", default="data/raw/olist")
    parser.add_argument("--batch-date", required=True)
    parser.add_argument("--batch-id")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dead-letter-max-rows", type=int, default=10)
    parser.add_argument("--dead-letter-max-rate", type=float, default=0.001)
    parser.add_argument("--no-clean", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    batch_id = args.batch_id or args.batch_date
    dead_letter_threshold = DeadLetterThreshold(
        max_rows=args.dead_letter_max_rows,
        max_rate=args.dead_letter_max_rate,
    )
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
        storage="local",
        dead_letter_threshold=dead_letter_threshold,
    )
    print(f"Wrote {manifest_path}")

    for prepared_file in prepared_files:
        print(
            f"Prepared {prepared_file.entity_name}: "
            f"{prepared_file.row_count} valid rows"
            f", {prepared_file.dead_letter_row_count} dead-letter rows"
            f" -> {prepared_file.local_path}"
        )

    assert_dead_letter_thresholds(prepared_files, dead_letter_threshold)


if __name__ == "__main__":
    main()
