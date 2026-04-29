# Architecture

## Goal

Provide a production-like batch analytics pipeline that can be run and reviewed
locally or against AWS infrastructure. The default workflow uses Python, local
files, PostgreSQL, Airflow, and dbt. An alternate workflow stages raw files in
S3 and loads them into Redshift before running the same dbt project on the
Redshift target.

## Flow

```text
Source archive
  -> source-contract validation
  -> row-level validation
  -> raw and dead-letter files
  -> PostgreSQL or Redshift raw tables
  -> reconciliation
  -> dbt transformations and tests
  -> analytical marts
```

Airflow coordinates both paths, while warehouse audit tables keep durable batch
state and quality results in PostgreSQL or Redshift.

## Components

### Ingestion

The ingestion layer reads `olist.zip`, verifies the expected files and headers,
validates row-level warehouse compatibility, adds operational metadata, and
writes gzip CSV files into a deterministic raw-zone layout:

```text
data/raw/olist/raw/<entity>/batch_date=<YYYY-MM-DD>/run_id=<run_id>/<entity>.csv.gz
data/raw/olist/dead_letter/<entity>/batch_date=<YYYY-MM-DD>/run_id=<run_id>/<entity>.csv.gz
```

The same logical layout is used for both execution modes. The default local
workflow writes to the filesystem, and the AWS workflow uploads the prepared
artifacts to S3 under the same partitioning scheme.

### Warehouse

The project supports two warehouse targets:

- PostgreSQL for the default local workflow
- Redshift for the AWS workflow

Both targets use the same logical schemas:

```text
raw_data
staging
intermediate
snapshots
core
marts
audit
```

Raw files are loaded into the `raw_data` schema. The `audit` schema stores batch
control state, raw load attempts, reconciliation results, dead-letter events,
and replay attempts for either warehouse target.

### Airflow

Airflow exposes two DAGs:

- `olist_modern_data_stack_local` for filesystem raw files plus PostgreSQL
- `olist_modern_data_stack_aws` for S3 raw files plus Redshift

Both DAGs follow the same high-level contracts:

```text
validate_source_contract
prepare_raw_files or upload_raw_files_to_s3
generate_correction_feeds
load_raw_files_to_postgres or load_raw_files_to_redshift
reconcile_raw_load
dbt_build
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
DBT_BUILT
```

`FAILED` is allowed from any state. The helper script prevents accidental
backward transitions.

### Reconciliation

After raw loading, the pipeline compares source counts, prepared rows, valid raw
rows, dead-letter rows, replayed rows, and rows present in the active warehouse
for the batch.

Core checks:

```text
prepared_total_rows = expected_source_rows
prepared_valid_rows + dead_letter_rows = prepared_total_rows
raw_loaded_rows = prepared_valid_rows + replayed_rows
```

A mismatch fails the DAG before dbt builds snapshots, facts, or marts.

## Execution Modes

The local workflow is still the default development entrypoint because it is
self-contained and easier to run in CI and on a laptop. The AWS workflow is now
also supported for manual runs with S3 and Redshift credentials. Both paths
share the same source contract, raw-zone partitioning, audit patterns, and dbt
models while varying only the storage and warehouse targets. CI intentionally
stays on the local PostgreSQL path so checks remain reproducible, self-contained,
and independent of cloud credentials or infrastructure availability.
