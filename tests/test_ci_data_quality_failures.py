from __future__ import annotations

import csv
import shutil
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from zipfile import ZIP_DEFLATED, ZipFile

from scripts.ingestion.local_storage import render_manifest
from scripts.ingestion.raw_files import prepare_entities
from scripts.ingestion.record_validation import (
    DeadLetterThreshold,
    DeadLetterThresholdExceeded,
    assert_dead_letter_thresholds,
)
from scripts.loading.load_raw_to_postgres import load_dead_letter_manifest_entries
from scripts.quality.reconcile_batch import ReconciliationInput, evaluate_reconciliation, fail_if_mismatched
from scripts.utilities.create_dead_letter_demo_archive import create_demo_archive
from scripts.utilities.validate_source_contract import load_contract, validate_archive


FIXTURE_ROOT = Path("tests") / "fixtures" / "olist_small"
FIXTURE_ARCHIVE = FIXTURE_ROOT / "olist_small.zip"
FIXTURE_PROFILE = FIXTURE_ROOT / "source_profile_small.json"


class FixtureContractFailureTests(unittest.TestCase):
    def test_missing_required_column_fails_source_contract(self) -> None:
        with temporary_workspace_directory() as tmpdir:
            broken_archive = Path(tmpdir) / "missing_customer_state.zip"
            write_archive_with_removed_column(
                source_archive=FIXTURE_ARCHIVE,
                output_archive=broken_archive,
                member_name="olist_customers_dataset.csv",
                removed_column="customer_state",
            )

            with self.assertRaisesRegex(ValueError, "customers: expected columns"):
                validate_archive(broken_archive, load_contract(FIXTURE_PROFILE))


class FixtureDeadLetterFailureTests(unittest.TestCase):
    def test_corrupt_fixture_row_is_dead_lettered_and_threshold_can_fail(self) -> None:
        with temporary_workspace_directory() as tmpdir:
            tmp_path = Path(tmpdir)
            corrupt_archive = tmp_path / "olist_small_bad_payment.zip"
            raw_dir = tmp_path / "raw"

            create_demo_archive(
                archive_path=FIXTURE_ARCHIVE,
                output_path=corrupt_archive,
                member_name="olist_order_payments_dataset.csv",
                column_name="payment_value",
                replacement_value="not_a_decimal",
                data_row_number=1,
            )

            prepared_files = prepare_entities(
                archive_path=corrupt_archive,
                profile_path=FIXTURE_PROFILE,
                output_dir=raw_dir,
                batch_date="2018-09-01",
                batch_id="2018-09-01",
                run_id="negative_test",
                clean=True,
            )
            threshold = DeadLetterThreshold(max_rows=0, max_rate=0)
            render_manifest(
                prepared_files,
                output_dir=raw_dir,
                manifest_name="manifest.json",
                storage="local",
                dead_letter_threshold=threshold,
            )

            manifest_entries = load_dead_letter_manifest_entries(raw_dir)
            payment_entry = manifest_entries["order_payments"]

            self.assertEqual(payment_entry.total_rows, 14)
            self.assertEqual(payment_entry.valid_rows, 13)
            self.assertEqual(payment_entry.failed_rows, 1)
            self.assertIn("payment_value: invalid decimal", payment_entry.reason_summary)

            with self.assertRaisesRegex(
                DeadLetterThresholdExceeded,
                "order_payments: 1 rejected rows > max_rows 0",
            ):
                assert_dead_letter_thresholds(prepared_files, threshold)


class ReconciliationGateFailureTests(unittest.TestCase):
    def test_reconciliation_gate_raises_on_failed_result(self) -> None:
        failed_result = evaluate_reconciliation(
            ReconciliationInput(
                entity_name="orders",
                source_uri="file:///orders.csv.gz",
                expected_source_rows=12,
                prepared_total_rows=12,
                prepared_valid_rows=12,
                dead_letter_rows=0,
                replayed_rows=0,
                raw_loaded_rows=11,
            )
        )

        with self.assertRaisesRegex(
            ValueError,
            "prepared_to_loaded_count_mismatch",
        ):
            fail_if_mismatched([failed_result])


def write_archive_with_removed_column(
    source_archive: Path,
    output_archive: Path,
    member_name: str,
    removed_column: str,
) -> None:
    with ZipFile(source_archive) as source_zip, ZipFile(
        output_archive,
        "w",
        compression=ZIP_DEFLATED,
    ) as output_zip:
        for source_info in source_zip.infolist():
            content = source_zip.read(source_info.filename)
            if source_info.filename != member_name:
                output_zip.writestr(source_info, content)
                continue

            rows = list(csv.DictReader(content.decode("utf-8").splitlines()))
            if not rows:
                raise AssertionError(f"Expected rows in {member_name}")

            fieldnames = [
                fieldname
                for fieldname in rows[0].keys()
                if fieldname != removed_column
            ]
            buffer = csv_string(fieldnames, rows)
            output_zip.writestr(member_name, buffer)


def csv_string(fieldnames: list[str], rows: list[dict[str, str]]) -> str:
    from io import StringIO

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@contextmanager
def temporary_workspace_directory() -> Iterator[str]:
    temp_root = Path("data") / "test_tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    temp_dir = temp_root / f"negative_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True)
    try:
        yield str(temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
