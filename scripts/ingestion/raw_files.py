"""Shared helpers for preparing Olist raw files.

The local raw zone intentionally mirrors the S3 key shape:

raw/<entity>/batch_date=<YYYY-MM-DD>/run_id=<run_id>/<file>.csv.gz

That keeps local Postgres loads and the optional S3 path on the same contract.
"""

from __future__ import annotations

import csv
import gzip
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable
from zipfile import ZipFile


SOURCE_SYSTEM = "olist_kaggle"
METADATA_COLUMNS = ["_batch_id", "_loaded_at", "_source_file", "_source_system"]


@dataclass(frozen=True)
class SourceEntity:
    file_name: str
    entity_name: str
    columns: list[str]


@dataclass(frozen=True)
class PreparedFile:
    entity_name: str
    file_name: str
    local_path: Path
    relative_path: str
    row_count: int


def utc_now_string() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_source_entities(profile_path: Path) -> list[SourceEntity]:
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    return [
        SourceEntity(
            file_name=entity["file_name"],
            entity_name=entity["entity_name"],
            columns=[column["name"] for column in entity["columns"]],
        )
        for entity in profile
    ]


def raw_relative_path(
    entity_name: str,
    batch_date: str,
    run_id: str,
    file_name: str,
) -> str:
    return (
        Path("raw")
        / entity_name
        / f"batch_date={batch_date}"
        / f"run_id={run_id}"
        / file_name
    ).as_posix()


def raw_file_path(
    output_dir: Path,
    entity_name: str,
    batch_date: str,
    run_id: str,
    file_name: str,
) -> Path:
    return output_dir / raw_relative_path(entity_name, batch_date, run_id, file_name)


def clean_entity_run_dirs(
    output_dir: Path,
    entity_names: Iterable[str],
    batch_date: str,
    run_id: str,
) -> None:
    for entity_name in entity_names:
        run_dir = (
            output_dir
            / "raw"
            / entity_name
            / f"batch_date={batch_date}"
            / f"run_id={run_id}"
        )
        if run_dir.exists():
            shutil.rmtree(run_dir)


def validate_archive(zip_file: ZipFile, entities: Iterable[SourceEntity]) -> None:
    archive_names = set(zip_file.namelist())
    missing_files = [
        entity.file_name
        for entity in entities
        if entity.file_name not in archive_names
    ]
    if missing_files:
        raise ValueError(f"Missing expected files: {', '.join(missing_files)}")


def validate_header(actual_header: list[str], entity: SourceEntity) -> None:
    if actual_header != entity.columns:
        raise ValueError(
            f"Unexpected header for {entity.file_name}. "
            f"Expected {entity.columns}, got {actual_header}"
        )


def prepare_entity(
    zip_file: ZipFile,
    entity: SourceEntity,
    output_dir: Path,
    batch_date: str,
    batch_id: str,
    run_id: str,
    loaded_at: str,
) -> PreparedFile:
    file_name = f"{entity.entity_name}.csv.gz"
    output_path = raw_file_path(
        output_dir,
        entity.entity_name,
        batch_date,
        run_id,
        file_name,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0

    with zip_file.open(entity.file_name) as raw_file:
        text_reader = (line.decode("utf-8-sig") for line in raw_file)
        reader = csv.DictReader(text_reader)

        if reader.fieldnames is None:
            raise ValueError(f"{entity.file_name} has no header row")

        validate_header(reader.fieldnames, entity)

        with gzip.open(output_path, mode="wt", encoding="utf-8", newline="") as output_file:
            fieldnames = [*entity.columns, *METADATA_COLUMNS]
            writer = csv.DictWriter(output_file, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                row["_batch_id"] = batch_id
                row["_loaded_at"] = loaded_at
                row["_source_file"] = entity.file_name
                row["_source_system"] = SOURCE_SYSTEM
                writer.writerow(row)
                row_count += 1

    return PreparedFile(
        entity_name=entity.entity_name,
        file_name=file_name,
        local_path=output_path,
        relative_path=raw_relative_path(entity.entity_name, batch_date, run_id, file_name),
        row_count=row_count,
    )


def prepare_entities(
    archive_path: Path,
    profile_path: Path,
    output_dir: Path,
    batch_date: str,
    batch_id: str,
    run_id: str,
    clean: bool,
) -> list[PreparedFile]:
    entities = load_source_entities(profile_path)
    if clean:
        clean_entity_run_dirs(
            output_dir,
            (entity.entity_name for entity in entities),
            batch_date,
            run_id,
        )

    loaded_at = utc_now_string()
    with ZipFile(archive_path) as zip_file:
        validate_archive(zip_file, entities)
        return [
            prepare_entity(
                zip_file=zip_file,
                entity=entity,
                output_dir=output_dir,
                batch_date=batch_date,
                batch_id=batch_id,
                run_id=run_id,
                loaded_at=loaded_at,
            )
            for entity in entities
        ]
