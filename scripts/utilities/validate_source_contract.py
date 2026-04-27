"""Validate the local Olist archive against the committed source contract."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile


@dataclass(frozen=True)
class ContractEntity:
    file_name: str
    entity_name: str
    row_count: int
    columns: list[str]


def load_contract(profile_path: Path) -> list[ContractEntity]:
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    return [
        ContractEntity(
            file_name=entity["file_name"],
            entity_name=entity["entity_name"],
            row_count=entity["row_count"],
            columns=[column["name"] for column in entity["columns"]],
        )
        for entity in profile
    ]


def read_header_and_count(zip_file: ZipFile, file_name: str) -> tuple[list[str], int]:
    with zip_file.open(file_name) as raw_file:
        reader = csv.DictReader(line.decode("utf-8-sig") for line in raw_file)

        if reader.fieldnames is None:
            raise ValueError(f"{file_name} has no header row")

        row_count = sum(1 for _ in reader)

    return list(reader.fieldnames), row_count


def validate_archive(archive_path: Path, entities: list[ContractEntity]) -> None:
    failures = []

    with ZipFile(archive_path) as zip_file:
        archive_names = set(zip_file.namelist())

        for entity in entities:
            if entity.file_name not in archive_names:
                failures.append(f"{entity.entity_name}: missing {entity.file_name}")
                continue

            header, row_count = read_header_and_count(zip_file, entity.file_name)

            if header != entity.columns:
                failures.append(
                    f"{entity.entity_name}: expected columns {entity.columns}, got {header}"
                )

            if row_count != entity.row_count:
                failures.append(
                    f"{entity.entity_name}: expected {entity.row_count} rows, got {row_count}"
                )

    if failures:
        joined_failures = "\n".join(f"- {failure}" for failure in failures)
        raise ValueError(f"Source contract validation failed:\n{joined_failures}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", default="olist.zip")
    parser.add_argument("--profile", default="docs/source_profile.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    entities = load_contract(Path(args.profile))
    validate_archive(Path(args.archive), entities)
    print(f"Validated {len(entities)} source files against {args.profile}")


if __name__ == "__main__":
    main()
