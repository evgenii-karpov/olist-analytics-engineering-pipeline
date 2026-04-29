# Architecture

## Goal

Provide a production-like batch analytics pipeline that can be run and reviewed
locally. The active path uses Python, local files, PostgreSQL, Airflow, and dbt;
AWS and Redshift are not required for the default workflow.

## Flow

```text
Source archive
  -> source-contract validation
  -> row-level validation
  -> raw and dead-letter files
  -> PostgreSQL raw tables
  -> reconciliation
  -> dbt transformations and tests
  -> analytical marts
```

Airflow coordinates the flow, while PostgreSQL audit tables keep durable batch
state and quality results.

## Components

### Ingestion

The ingestion layer reads `olist.zip`, verifies the expected files and headers,
validates row-level warehouse compatibility, adds operational metadata, and
writes gzip CSV files into a deterministic raw-zone layout:

```text
data/raw/olist/raw/<entity>/batch_date=<YYYY-MM-DD>/run_id=<run_id>/<entity>.csv.gz
data/raw/olist/dead_letter/<entity>/batch_date=<YYYY-MM-DD>/run_id=<run_id>/<entity>.csv.gz
```

The same logical layout can map to object storage, but the local project uses
the filesystem so the full pipeline stays reproducible.

### Warehouse

PostgreSQL is the local analytical warehouse. It owns these schemas:

```text
raw_data
staging
intermediate
snapshots
core
marts
audit
```

Raw files are streamed into the `raw_data` schema. The `audit` schema stores batch
control state, raw load attempts, reconciliation results, dead-letter events,
and replay attempts.

### Airflow

The local DAG is `olist_modern_data_stack_local`. Its task boundaries mirror the
major pipeline contracts:

```text
validate_source_contract
prepare_raw_files
generate_correction_feeds
load_raw_files_to_postgres
reconcile_raw_load
dbt_build_snapshot_inputs
dbt_snapshot
dbt_build
dbt_test
```

Airflow handles orchestration, retries, parameters, and failure callbacks. The
warehouse remains the durable source of batch status.

### dbt

dbt owns the analytical transformation layer:

```text
sources -> staging -> intermediate -> snapshots -> core -> marts
```

The modeling details, grain decisions, SCD2 strategy, and mart definitions live
in [data_model.md](data_model.md).

## Reliability Patterns

### Source Contract

Missing files, changed headers, and changed source row counts are structural
contract failures. They fail before raw loading starts.

The generated contract is documented in [source_contract.md](source_contract.md).

### Dead Letter Pattern

Record-level type and length failures are written to the dead-letter zone with
the source row number, failure stage, reason, and timestamp. A run continues
only while rejected rows remain within configured thresholds.

Corrected dead-letter files can be replayed into raw tables. Replays are
idempotent for a stable replay id and are recorded in
`audit.dead_letter_replays`.

### Batch Control

`audit.batch_runs` tracks each logical batch independently of Airflow task
history.

```text
STARTED
SOURCE_VALIDATED
RAW_PREPARED
RAW_LOADED
RAW_RECONCILED
DBT_SNAPSHOT_INPUTS_BUILT
DBT_SNAPSHOTTED
DBT_BUILT
TESTED
```

`FAILED` is allowed from any state. The helper script prevents accidental
backward transitions.

### Reconciliation

After raw loading, the pipeline compares source counts, prepared rows, valid raw
rows, dead-letter rows, replayed rows, and rows present in PostgreSQL for the
batch.

Core checks:

```text
prepared_total_rows = expected_source_rows
prepared_valid_rows + dead_letter_rows = prepared_total_rows
raw_loaded_rows = prepared_valid_rows + replayed_rows
```

A mismatch fails the DAG before dbt builds snapshots, facts, or marts.

## Preserved Redshift Artifacts

The active project does not assume Redshift access. The repository still keeps
reference Redshift DDL, COPY templates, a Redshift dbt profile target, and the
older S3-to-Redshift DAG so the original design discussion remains visible.
