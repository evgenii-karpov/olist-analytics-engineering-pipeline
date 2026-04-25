select
    reconciliation_run_id,
    batch_id,
    entity_name,
    expected_source_rows,
    prepared_total_rows,
    prepared_valid_rows,
    dead_letter_rows,
    replayed_rows,
    expected_loaded_rows,
    raw_loaded_rows,
    source_to_prepared_delta,
    prepared_to_loaded_delta,
    status,
    failed_checks,
    created_at
from audit.batch_reconciliation
order by created_at desc, entity_name asc
