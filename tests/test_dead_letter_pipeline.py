from __future__ import annotations

import csv
import gzip
import json
import shutil
import unittest
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

from scripts.ingestion.local_storage import render_manifest
from scripts.ingestion.raw_files import PreparedFile, write_validated_rows
from scripts.ingestion.record_validation import (
    DeadLetterThreshold,
    DeadLetterThresholdExceeded,
    assert_dead_letter_thresholds,
    validate_row,
)
from scripts.loading.load_raw_to_postgres import (
    RawLoadSpec,
    load_dead_letter_manifest_entries,
)
from scripts.loading.replay_dead_letters import ReplaySpec, build_replay_payload
from scripts.orchestration.batch_control import (
    ManifestUris,
    manifest_uris,
    validate_transition,
)
from scripts.quality.reconcile_batch import (
    ReconciliationInput,
    build_reconciliation_results,
    evaluate_reconciliation,
)
from scripts.utilities.create_dead_letter_demo_archive import create_demo_archive


class RecordValidationTests(unittest.TestCase):
    def test_validate_row_matches_postgres_compatible_cast_boundaries(self) -> None:
        column_types = {
            "id": "integer",
            "price": "decimal(18, 2)",
            "latitude": "decimal(18, 14)",
            "created_at": "timestamp",
            "code": "varchar(4)",
        }

        valid_errors = validate_row(
            {
                "id": "42",
                "price": "10.129",
                "latitude": "-23.54562128115268",
                "created_at": "2018-09-01 12:34:56",
                "code": "ABCD",
            },
            column_types,
        )

        self.assertEqual(valid_errors, [])

        invalid_errors = validate_row(
            {
                "id": "not_an_int",
                "price": "not_a_decimal",
                "latitude": "123456.12345678901234",
                "created_at": "yesterday",
                "code": "ABCDE",
            },
            column_types,
        )

        self.assertIn("id: invalid integer 'not_an_int'", invalid_errors)
        self.assertIn("price: invalid decimal 'not_a_decimal'", invalid_errors)
        self.assertIn(
            "latitude: decimal integer digits 6 exceed decimal(18, 14)",
            invalid_errors,
        )
        self.assertIn("created_at: invalid timestamp 'yesterday'", invalid_errors)
        self.assertIn("code: value length 5 exceeds varchar(4)", invalid_errors)

    def test_dead_letter_threshold_checks_both_count_and_rate(self) -> None:
        threshold = DeadLetterThreshold(max_rows=1, max_rate=0.1)
        prepared_files = [
            PreparedFile(
                entity_name="order_payments",
                file_name="order_payments.csv.gz",
                local_path=Path("order_payments.csv.gz"),
                relative_path="raw/order_payments.csv.gz",
                row_count=8,
                dead_letter_row_count=2,
                total_row_count=10,
            )
        ]

        with self.assertRaisesRegex(
            DeadLetterThresholdExceeded,
            "order_payments: 2 rejected rows > max_rows 1",
        ):
            assert_dead_letter_thresholds(prepared_files, threshold)


class RawDeadLetterFileTests(unittest.TestCase):
    def test_write_validated_rows_splits_bad_rows_and_manifest_is_loadable(
        self,
    ) -> None:
        with temporary_workspace_directory() as tmpdir:
            output_dir = Path(tmpdir)
            prepared_file = write_validated_rows(
                rows=[
                    (
                        2,
                        {
                            "payment_id": "1",
                            "payment_value": "18.12",
                            "paid_at": "2018-09-01 00:00:00",
                        },
                    ),
                    (
                        3,
                        {
                            "payment_id": "2",
                            "payment_value": "not_a_decimal",
                            "paid_at": "2018-09-01 00:00:00",
                        },
                    ),
                ],
                output_dir=output_dir,
                entity_name="order_payments",
                file_name="order_payments.csv.gz",
                columns=["payment_id", "payment_value", "paid_at"],
                column_types={
                    "payment_id": "integer",
                    "payment_value": "decimal(18, 2)",
                    "paid_at": "timestamp",
                },
                batch_date="2018-09-01",
                batch_id="2018-09-01",
                run_id="unit_test",
                loaded_at="2026-04-24T00:00:00Z",
                source_file="payments.csv",
                source_system="unit_test",
                dead_letter_stage="raw_preparation",
            )

            self.assertEqual(prepared_file.row_count, 1)
            self.assertEqual(prepared_file.total_row_count, 2)
            self.assertEqual(prepared_file.dead_letter_row_count, 1)
            self.assertIsNotNone(prepared_file.dead_letter_path)

            raw_rows = read_gzip_csv(prepared_file.local_path)
            self.assertEqual(len(raw_rows), 1)
            self.assertEqual(raw_rows[0]["payment_id"], "1")
            self.assertEqual(raw_rows[0]["_batch_id"], "2018-09-01")

            dead_letter_rows = read_gzip_csv(prepared_file.dead_letter_path)
            self.assertEqual(len(dead_letter_rows), 1)
            self.assertEqual(dead_letter_rows[0]["_source_row_number"], "3")
            self.assertEqual(
                dead_letter_rows[0]["_dead_letter_reason"],
                "payment_value: invalid decimal 'not_a_decimal'",
            )

            threshold = DeadLetterThreshold(max_rows=10, max_rate=0.5)
            render_manifest(
                [prepared_file],
                output_dir=output_dir,
                manifest_name="manifest.json",
                storage="local",
                dead_letter_threshold=threshold,
            )

            manifest_entries = load_dead_letter_manifest_entries(output_dir)
            manifest_entry = manifest_entries["order_payments"]
            self.assertEqual(manifest_entry.valid_rows, 1)
            self.assertEqual(manifest_entry.failed_rows, 1)
            self.assertEqual(manifest_entry.threshold_max_rows, 10)
            self.assertEqual(manifest_entry.threshold_max_rate, 0.5)
            self.assertEqual(
                json.loads(manifest_entry.reason_summary),
                {"payment_value: invalid decimal 'not_a_decimal'": 1},
            )


class DeadLetterReplayTests(unittest.TestCase):
    def test_replay_payload_revalidates_fixed_rows_and_sets_replay_metadata(
        self,
    ) -> None:
        spec = ReplaySpec(
            entity_name="order_payments",
            columns=["payment_id", "payment_value"],
            column_types={
                "payment_id": "integer",
                "payment_value": "decimal(18, 2)",
            },
        )

        with self.assertRaisesRegex(
            ValueError,
            "Corrected dead-letter file still contains invalid rows",
        ):
            build_replay_payload(
                spec=spec,
                rows=[
                    {
                        "payment_id": "1",
                        "payment_value": "not_a_decimal",
                        "_source_row_number": "2",
                    }
                ],
                batch_id="2018-09-01",
                replay_id="payment_fix",
                source_system="unit_replay",
            )

        payload = build_replay_payload(
            spec=spec,
            rows=[
                {
                    "payment_id": "1",
                    "payment_value": "18.12",
                    "_source_row_number": "2",
                }
            ],
            batch_id="2018-09-01",
            replay_id="payment_fix",
            source_system="unit_replay",
        )

        replay_rows = list(csv.DictReader(payload.csv_buffer))
        self.assertEqual(payload.row_count, 1)
        self.assertEqual(payload.replay_source_file, "dead_letter_replay:payment_fix")
        self.assertEqual(replay_rows[0]["payment_value"], "18.12")
        self.assertEqual(replay_rows[0]["_batch_id"], "2018-09-01")
        self.assertEqual(replay_rows[0]["_source_file"], payload.replay_source_file)
        self.assertEqual(replay_rows[0]["_source_system"], "unit_replay")


class DeadLetterDemoArchiveTests(unittest.TestCase):
    def test_demo_archive_corrupts_only_requested_cell(self) -> None:
        with temporary_workspace_directory() as tmpdir:
            root = Path(tmpdir)
            source_archive = root / "source.zip"
            output_archive = root / "demo.zip"

            with ZipFile(source_archive, "w") as archive:
                archive.writestr(
                    "payments.csv",
                    "id,payment_value\n1,10.00\n2,20.00\n",
                )
                archive.writestr("untouched.csv", "id,name\n1,ok\n")

            create_demo_archive(
                archive_path=source_archive,
                output_path=output_archive,
                member_name="payments.csv",
                column_name="payment_value",
                replacement_value="not_a_decimal",
                data_row_number=2,
            )

            with ZipFile(output_archive) as archive:
                payments = archive.read("payments.csv").decode("utf-8")
                untouched = archive.read("untouched.csv").decode("utf-8")

            self.assertEqual(
                payments,
                "id,payment_value\n1,10.00\n2,not_a_decimal\n",
            )
            self.assertEqual(untouched, "id,name\n1,ok\n")


class BatchControlTests(unittest.TestCase):
    def test_validate_transition_allows_forward_and_blocks_backward_moves(self) -> None:
        validate_transition(None, "STARTED")
        validate_transition("STARTED", "SOURCE_VALIDATED")
        validate_transition("RAW_LOADED", "RAW_LOADED")
        validate_transition("DBT_BUILT", "FAILED")

        with self.assertRaisesRegex(ValueError, "Cannot move batch backwards"):
            validate_transition("RAW_RECONCILED", "RAW_LOADED")

        with self.assertRaisesRegex(
            ValueError, "Cannot move batch from terminal status"
        ):
            validate_transition("TESTED", "RAW_LOADED")

    def test_manifest_uris_are_optional_and_detect_existing_manifests(self) -> None:
        with temporary_workspace_directory() as tmpdir:
            raw_dir = Path(tmpdir)
            self.assertEqual(
                manifest_uris(raw_dir),
                ManifestUris(raw_manifest_uri=None, correction_manifest_uri=None),
            )

            (raw_dir / "manifest.json").write_text("{}", encoding="utf-8")
            uris = manifest_uris(raw_dir)

            raw_manifest_uri = uris.raw_manifest_uri
            self.assertIsNotNone(raw_manifest_uri)
            assert raw_manifest_uri is not None
            self.assertTrue(raw_manifest_uri.endswith("/manifest.json"))
            self.assertIsNone(uris.correction_manifest_uri)


class ReconciliationTests(unittest.TestCase):
    def test_evaluate_reconciliation_passes_when_counts_balance(self) -> None:
        result = evaluate_reconciliation(
            ReconciliationInput(
                entity_name="order_payments",
                source_uri="file:///payments.csv.gz",
                expected_source_rows=10,
                prepared_total_rows=10,
                prepared_valid_rows=9,
                dead_letter_rows=1,
                replayed_rows=1,
                raw_loaded_rows=10,
            )
        )

        self.assertEqual(result.status, "PASS")
        self.assertIsNone(result.failed_checks)
        self.assertEqual(result.expected_loaded_rows, 10)
        self.assertEqual(result.source_to_prepared_delta, 0)
        self.assertEqual(result.prepared_to_loaded_delta, 0)

    def test_evaluate_reconciliation_fails_on_manifest_and_load_mismatch(self) -> None:
        result = evaluate_reconciliation(
            ReconciliationInput(
                entity_name="orders",
                source_uri="file:///orders.csv.gz",
                expected_source_rows=10,
                prepared_total_rows=9,
                prepared_valid_rows=8,
                dead_letter_rows=0,
                replayed_rows=0,
                raw_loaded_rows=7,
            )
        )

        self.assertEqual(result.status, "FAIL")
        self.assertIn("source_to_prepared_count_mismatch", result.failed_checks or "")
        self.assertIn(
            "valid_plus_dead_letter_count_mismatch", result.failed_checks or ""
        )
        self.assertIn("prepared_to_loaded_count_mismatch", result.failed_checks or "")

    def test_build_reconciliation_results_uses_manifest_totals_for_corrections(
        self,
    ) -> None:
        specs = [
            RawLoadSpec(
                entity_name="customer_profile_changes",
                file_name="customer_profile_changes.csv.gz",
            ),
        ]
        manifest_entry = SimpleNamespace(
            entity_name="customer_profile_changes",
            source_uri="file:///customer_profile_changes.csv.gz",
            total_rows=20,
            valid_rows=20,
            failed_rows=0,
        )

        results = build_reconciliation_results(
            specs=specs,
            expected_source_rows={},
            manifest_entries={"customer_profile_changes": manifest_entry},
            raw_loaded_rows={"customer_profile_changes": 20},
            replayed_rows={},
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].expected_source_rows, 20)
        self.assertEqual(results[0].status, "PASS")


def read_gzip_csv(path: Path | None) -> list[dict[str, str]]:
    if path is None:
        raise AssertionError("Expected gzip CSV path to be present")

    with gzip.open(path, mode="rt", encoding="utf-8", newline="") as csv_file:
        return list(csv.DictReader(csv_file))


@contextmanager
def temporary_workspace_directory() -> Iterator[str]:
    temp_root = Path("data") / "test_tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    temp_dir = temp_root / f"unit_{uuid.uuid4().hex}"
    temp_dir.mkdir(parents=True)
    try:
        yield str(temp_dir)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
