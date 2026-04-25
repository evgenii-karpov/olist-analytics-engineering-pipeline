"""Row-level validation helpers for raw ingestion dead-letter handling."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation

TYPE_PATTERN = re.compile(r"^(?P<base>[a-z]+)(?:\((?P<args>[^)]+)\))?$")


class DeadLetterThresholdExceeded(ValueError):
    """Raised when rejected records exceed the configured threshold."""


@dataclass(frozen=True)
class DeadLetterThreshold:
    max_rows: int
    max_rate: float

    def __post_init__(self) -> None:
        if self.max_rows < 0:
            raise ValueError("dead-letter max rows must be non-negative")
        if self.max_rate < 0 or self.max_rate > 1:
            raise ValueError("dead-letter max rate must be between 0 and 1")

    def as_manifest(self) -> dict[str, int | float]:
        return {
            "max_rows": self.max_rows,
            "max_rate": self.max_rate,
        }

    def violation_message(
        self,
        entity_name: str,
        dead_letter_rows: int,
        total_rows: int,
    ) -> str | None:
        if dead_letter_rows == 0:
            return None

        dead_letter_rate = dead_letter_rows / total_rows if total_rows else 0
        failures = []
        if dead_letter_rows > self.max_rows:
            failures.append(
                f"{dead_letter_rows} rejected rows > max_rows {self.max_rows}"
            )
        if dead_letter_rate > self.max_rate:
            failures.append(
                f"{dead_letter_rate:.6f} rejected rate > max_rate {self.max_rate:.6f}"
            )

        if not failures:
            return None

        return f"{entity_name}: " + "; ".join(failures)


def assert_dead_letter_thresholds(
    prepared_files: list[object],
    threshold: DeadLetterThreshold,
) -> None:
    violations = [
        message
        for prepared_file in prepared_files
        if (
            message := threshold.violation_message(
                entity_name=prepared_file.entity_name,
                dead_letter_rows=prepared_file.dead_letter_row_count,
                total_rows=prepared_file.total_row_count,
            )
        )
    ]
    if violations:
        joined = "\n".join(f"- {violation}" for violation in violations)
        raise DeadLetterThresholdExceeded(
            "Dead-letter threshold exceeded:\n"
            f"{joined}\n"
            "Valid rows were written to raw files and rejected rows were written "
            "to the dead-letter zone for inspection."
        )


def validate_row(
    row: dict[str, str],
    column_types: dict[str, str],
) -> list[str]:
    errors = []
    for column_name, raw_type in column_types.items():
        value = row.get(column_name)
        if value is None:
            errors.append(f"{column_name}: missing column")
            continue

        if value == "":
            continue

        errors.extend(validate_value(column_name, value, raw_type))

    return errors


def validate_value(column_name: str, value: str, raw_type: str) -> list[str]:
    match = TYPE_PATTERN.match(raw_type.strip().lower())
    if not match:
        return [f"{column_name}: unsupported raw type {raw_type}"]

    base_type = match.group("base")
    args = parse_type_args(match.group("args"))

    if base_type == "varchar":
        return validate_varchar(column_name, value, args)
    if base_type == "integer":
        return validate_integer(column_name, value)
    if base_type == "decimal":
        return validate_decimal(column_name, value, args)
    if base_type == "timestamp":
        return validate_timestamp(column_name, value)

    return [f"{column_name}: unsupported raw type {raw_type}"]


def parse_type_args(raw_args: str | None) -> list[int]:
    if raw_args is None:
        return []
    return [int(value.strip()) for value in raw_args.split(",")]


def validate_varchar(column_name: str, value: str, args: list[int]) -> list[str]:
    if not args:
        return []

    max_length = args[0]
    if len(value) > max_length:
        return [
            f"{column_name}: value length {len(value)} exceeds varchar({max_length})"
        ]
    return []


def validate_integer(column_name: str, value: str) -> list[str]:
    try:
        int(value)
    except ValueError:
        return [f"{column_name}: invalid integer {value!r}"]
    return []


def validate_decimal(column_name: str, value: str, args: list[int]) -> list[str]:
    try:
        decimal_value = Decimal(value)
    except InvalidOperation:
        return [f"{column_name}: invalid decimal {value!r}"]

    if not decimal_value.is_finite():
        return [f"{column_name}: invalid decimal {value!r}"]

    if len(args) != 2:
        return []

    precision, scale = args
    _, digits, exponent = decimal_value.as_tuple()
    actual_scale = max(-exponent, 0)
    integer_digits = max(len(digits) - actual_scale, 0)
    max_integer_digits = precision - scale

    if integer_digits > max_integer_digits:
        return [
            f"{column_name}: decimal integer digits {integer_digits} exceed decimal({precision}, {scale})"
        ]
    return []


def validate_timestamp(column_name: str, value: str) -> list[str]:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return [f"{column_name}: invalid timestamp {value!r}"]
    return []
