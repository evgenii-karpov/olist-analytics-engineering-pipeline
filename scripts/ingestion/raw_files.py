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
from collections.abc import Iterable, Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from zipfile import ZipFile

from scripts.ingestion.record_validation import validate_row

SOURCE_SYSTEM = "olist_kaggle"
METADATA_COLUMNS = ["_batch_id", "_loaded_at", "_source_file", "_source_system"]
DEAD_LETTER_METADATA_COLUMNS = [
    "_source_row_number",
    "_dead_letter_stage",
    "_dead_letter_reason",
    "_dead_lettered_at",
]


@dataclass(frozen=True)
class SourceEntity:
    file_name: str
    entity_name: str
    columns: list[str]
    column_types: dict[str, str]


@dataclass(frozen=True)
class PreparedFile:
    entity_name: str
    file_name: str
    local_path: Path
    relative_path: str
    row_count: int
    total_row_count: int
    dead_letter_path: Path | None = None
    dead_letter_relative_path: str | None = None
    dead_letter_row_count: int = 0
    dead_letter_reason_counts: dict[str, int] | None = None


def utc_now_string() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_source_entities(profile_path: Path) -> list[SourceEntity]:
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    return [
        SourceEntity(
            file_name=entity["file_name"],
            entity_name=entity["entity_name"],
            columns=[column["name"] for column in entity["columns"]],
            column_types={
                column["name"]: column["redshift_raw_type"]
                for column in entity["columns"]
            },
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


def dead_letter_relative_path(
    entity_name: str,
    batch_date: str,
    run_id: str,
    file_name: str,
) -> str:
    return (
        Path("dead_letter")
        / entity_name
        / f"batch_date={batch_date}"
        / f"run_id={run_id}"
        / file_name
    ).as_posix()


def dead_letter_file_path(
    output_dir: Path,
    entity_name: str,
    batch_date: str,
    run_id: str,
    file_name: str,
) -> Path:
    return output_dir / dead_letter_relative_path(
        entity_name,
        batch_date,
        run_id,
        file_name,
    )


def clean_entity_run_dirs(
    output_dir: Path,
    entity_names: Iterable[str],
    batch_date: str,
    run_id: str,
) -> None:
    for entity_name in entity_names:
        for zone in ("raw", "dead_letter"):
            run_dir = (
                output_dir
                / zone
                / entity_name
                / f"batch_date={batch_date}"
                / f"run_id={run_id}"
            )
            if run_dir.exists():
                shutil.rmtree(run_dir)


def validate_archive(zip_file: ZipFile, entities: Iterable[SourceEntity]) -> None:
    archive_names = set(zip_file.namelist())
    missing_files = [
        entity.file_name for entity in entities if entity.file_name not in archive_names
    ]
    if missing_files:
        raise ValueError(f"Missing expected files: {', '.join(missing_files)}")


def validate_header(actual_header: Sequence[str], entity: SourceEntity) -> None:
    if list(actual_header) != entity.columns:
        raise ValueError(
            f"Unexpected header for {entity.file_name}. "
            f"Expected {entity.columns}, got {actual_header}"
        )


def increment_reason_count(
    reason_counts: dict[str, int],
    validation_errors: list[str],
) -> None:
    reason_key = validation_errors[0] if validation_errors else "unknown"
    reason_counts[reason_key] = reason_counts.get(reason_key, 0) + 1


def write_validated_rows(
    rows: Iterable[tuple[int, dict[str, str]]],
    output_dir: Path,
    entity_name: str,
    file_name: str,
    columns: list[str],
    column_types: dict[str, str],
    batch_date: str,
    batch_id: str,
    run_id: str,
    loaded_at: str,
    source_file: str,
    source_system: str,
    dead_letter_stage: str,
) -> PreparedFile:
    output_path = raw_file_path(output_dir, entity_name, batch_date, run_id, file_name)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dead_letter_path = dead_letter_file_path(
        output_dir,
        entity_name,
        batch_date,
        run_id,
        file_name,
    )
    dead_letter_writer = None

    row_count = 0
    total_row_count = 0
    dead_letter_row_count = 0
    reason_counts: dict[str, int] = {}
    dead_lettered_at = utc_now_string()

    with ExitStack() as stack:
        output_file = stack.enter_context(
            gzip.open(output_path, mode="wt", encoding="utf-8", newline="")
        )
        fieldnames = [*columns, *METADATA_COLUMNS]
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for source_row_number, row in rows:
            total_row_count += 1
            validation_errors = validate_row(row, column_types)

            if validation_errors:
                if dead_letter_writer is None:
                    dead_letter_path.parent.mkdir(parents=True, exist_ok=True)
                    dead_letter_file = stack.enter_context(
                        gzip.open(
                            dead_letter_path,
                            mode="wt",
                            encoding="utf-8",
                            newline="",
                        )
                    )
                    dead_letter_writer = csv.DictWriter(
                        dead_letter_file,
                        fieldnames=[
                            *columns,
                            *METADATA_COLUMNS,
                            *DEAD_LETTER_METADATA_COLUMNS,
                        ],
                    )
                    dead_letter_writer.writeheader()

                dead_letter_row = {column: row.get(column, "") for column in columns}
                dead_letter_row["_batch_id"] = batch_id
                dead_letter_row["_loaded_at"] = loaded_at
                dead_letter_row["_source_file"] = source_file
                dead_letter_row["_source_system"] = source_system
                dead_letter_row["_source_row_number"] = str(source_row_number)
                dead_letter_row["_dead_letter_stage"] = dead_letter_stage
                dead_letter_row["_dead_letter_reason"] = "; ".join(validation_errors)
                dead_letter_row["_dead_lettered_at"] = dead_lettered_at
                dead_letter_writer.writerow(dead_letter_row)

                dead_letter_row_count += 1
                increment_reason_count(reason_counts, validation_errors)
                continue

            row = dict(row)
            row["_batch_id"] = batch_id
            row["_loaded_at"] = loaded_at
            row["_source_file"] = source_file
            row["_source_system"] = source_system
            writer.writerow(row)
            row_count += 1

    return PreparedFile(
        entity_name=entity_name,
        file_name=file_name,
        local_path=output_path,
        relative_path=raw_relative_path(entity_name, batch_date, run_id, file_name),
        row_count=row_count,
        total_row_count=total_row_count,
        dead_letter_path=dead_letter_path if dead_letter_row_count else None,
        dead_letter_relative_path=(
            dead_letter_relative_path(entity_name, batch_date, run_id, file_name)
            if dead_letter_row_count
            else None
        ),
        dead_letter_row_count=dead_letter_row_count,
        dead_letter_reason_counts=reason_counts if dead_letter_row_count else None,
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

    with zip_file.open(entity.file_name) as raw_file:
        text_reader = (line.decode("utf-8-sig") for line in raw_file)
        reader = csv.DictReader(text_reader)

        if reader.fieldnames is None:
            raise ValueError(f"{entity.file_name} has no header row")

        validate_header(reader.fieldnames, entity)

        return write_validated_rows(
            rows=enumerate(reader, start=2),
            output_dir=output_dir,
            entity_name=entity.entity_name,
            file_name=file_name,
            columns=entity.columns,
            column_types=entity.column_types,
            batch_date=batch_date,
            batch_id=batch_id,
            run_id=run_id,
            loaded_at=loaded_at,
            source_file=entity.file_name,
            source_system=SOURCE_SYSTEM,
            dead_letter_stage="raw_preparation",
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
