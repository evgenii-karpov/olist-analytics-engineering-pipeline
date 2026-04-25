"""S3 storage helpers for prepared raw files."""

from __future__ import annotations

from collections.abc import Iterable

from scripts.ingestion.raw_files import PreparedFile


def s3_key_for(prefix: str, relative_path: str) -> str:
    normalized_prefix = prefix.strip("/")
    if not normalized_prefix:
        return relative_path
    return f"{normalized_prefix}/{relative_path}"


def s3_uri_for(bucket: str, prefix: str | None, relative_path: str) -> str:
    return f"s3://{bucket}/{s3_key_for(prefix or '', relative_path)}"


def upload_files_to_s3(
    bucket: str,
    prefix: str,
    prepared_files: Iterable[PreparedFile],
) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for S3 upload. Install project dependencies first."
        ) from exc

    s3_client = boto3.client("s3")
    for prepared_file in prepared_files:
        s3_key = s3_key_for(prefix, prepared_file.relative_path)
        s3_client.upload_file(str(prepared_file.local_path), bucket, s3_key)
        print(f"Uploaded s3://{bucket}/{s3_key}")

        if prepared_file.dead_letter_path and prepared_file.dead_letter_relative_path:
            dead_letter_key = s3_key_for(
                prefix, prepared_file.dead_letter_relative_path
            )
            s3_client.upload_file(
                str(prepared_file.dead_letter_path),
                bucket,
                dead_letter_key,
            )
            print(f"Uploaded s3://{bucket}/{dead_letter_key}")
