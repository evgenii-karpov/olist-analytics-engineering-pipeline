"""Local filesystem storage helpers for prepared raw files."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.ingestion.raw_files import PreparedFile, utc_now_string
from scripts.ingestion.s3_storage import s3_uri_for


def local_uri_for(path: Path) -> str:
    return path.resolve().as_uri()


def render_manifest(
    prepared_files: list[PreparedFile],
    output_dir: Path,
    manifest_name: str,
    storage: str,
    s3_bucket: str | None = None,
    s3_prefix: str | None = None,
) -> Path:
    manifest = {
        "generated_at": utc_now_string(),
        "storage": storage,
        "bucket": s3_bucket,
        "s3_prefix": s3_prefix,
        "files": [
            {
                "entity_name": prepared_file.entity_name,
                "file_name": prepared_file.file_name,
                "relative_path": prepared_file.relative_path,
                "local_path": str(prepared_file.local_path),
                "local_uri": local_uri_for(prepared_file.local_path),
                "s3_uri": (
                    s3_uri_for(s3_bucket, s3_prefix, prepared_file.relative_path)
                    if s3_bucket and s3_prefix
                    else None
                ),
                "row_count": prepared_file.row_count,
            }
            for prepared_file in prepared_files
        ],
    }
    manifest_path = output_dir / manifest_name
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path
