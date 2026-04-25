# Architecture

## Goal

Build a production-like batch analytics pipeline for the Olist e-commerce
dataset that can run fully locally. The active stack is Python, a local
S3-shaped raw zone, PostgreSQL 18.3 in Docker, Apache Airflow 3.2.1, and dbt.

The original AWS S3 + Redshift design remains documented under `infra/redshift`
and `infra/aws`, but it is no longer required for the main development loop.

## High-Level Flow

```text
Local source archive
  -> Python ingestion
  -> row-level validation and dead-letter files
  -> local raw zone
  -> PostgreSQL raw schema
  -> dbt staging schema
  -> dbt snapshots schema
  -> dbt core schema
  -> dbt marts schema
```

## Components

### Python Ingestion

Python reads `olist.zip`, validates expected CSV files, adds operational
metadata, writes gzip CSV files, and emits manifests.

The local raw path deliberately mirrors the old S3 object key shape:

```text
data/raw/olist/raw/<entity>/batch_date=<YYYY-MM-DD>/run_id=<run_id>/<entity>.csv.gz
data/raw/olist/dead_letter/<entity>/batch_date=<YYYY-MM-DD>/run_id=<run_id>/<entity>.csv.gz
```

Responsibilities:

- Validate that all expected source files exist.
- Validate column names before load.
- Validate record-level warehouse compatibility for integers, decimals,
  timestamps, and varchar lengths before `COPY`.
- Generate deterministic raw file paths.
- Add `_batch_id`, `_loaded_at`, `_source_file`, and `_source_system`.
- Write rejected records with `_source_row_number`, `_dead_letter_stage`,
  `_dead_letter_reason`, and `_dead_lettered_at`.
- Enforce threshold mode using max rejected rows and max rejected rate.
- Keep `_batch_id` stable for a logical batch while `run_id` identifies the
  individual Airflow/manual attempt.
- Support repeatable manual runs and Airflow scheduled runs.
- Keep S3 upload available as an optional wrapper script.

### Local Raw Zone

The local raw zone is the filesystem-backed equivalent of an object-storage
landing zone. It keeps immutable, partitioned files and separates ingestion from
warehouse loading.

This gives the project a portable contract:

```text
raw path contract stays stable
storage implementation can be local filesystem or S3
```

Dead-letter files use the same partitioning scheme as raw files. That keeps
bad records inspectable and replayable without mixing them into the successful
raw load.

### PostgreSQL

PostgreSQL 18.3 is the default local warehouse.

Schemas:

```text
raw
staging
intermediate
snapshots
core
marts
audit
```

Raw files are loaded with PostgreSQL `COPY FROM STDIN`, streamed from Python so
gzip files do not need to be manually extracted.

The audit schema also owns batch-level control metadata. `audit.batch_runs`
stores the current state of each logical batch independently of Airflow task
history.

### Apache Airflow

Airflow orchestrates the local pipeline with a dedicated DAG:

```text
olist_modern_data_stack_local
```

Task flow:

```text
start_batch
validate_source_contract
mark_source_validated
prepare_raw_files
generate_correction_feeds
mark_raw_prepared
load_raw_files_to_postgres
reconcile_raw_load
dbt_build_snapshot_inputs
mark_snapshot_inputs_built
dbt_snapshot
mark_dbt_snapshotted
dbt_build
mark_dbt_built
dbt_test
mark_tested
```

The previous AWS DAG is preserved separately as:

```text
olist_modern_data_stack
```

### dbt

dbt owns transformations, tests, snapshots, docs, and lineage.

Layer strategy:

```text
sources      raw PostgreSQL tables
staging      typed, renamed, lightly cleaned source models
intermediate reusable transformation models
snapshots    SCD2 history tables
core         dimensional model and star schema
marts        business-facing aggregates
```

The default dbt profile target is `local_pg`. A `redshift` target is retained for
future AWS work. SQL dialect differences are isolated in macros when a shared
model can be kept portable.

## Raw Load Strategy

Each run writes raw files and then loads them idempotently:

1. Use `batch_date` as the default stable local `_batch_id`.
2. Validate source rows and split invalid rows into the dead-letter zone.
3. Fail the ingestion task if rejected rows exceed the threshold.
4. Delete rows for the current `_batch_id` from the target raw table.
5. Delete the matching audit rows for the logical batch and entity.
6. Insert `audit.dead_letter_events` when a within-threshold rejection exists.
7. Stream the valid gzip CSV into PostgreSQL with `COPY FROM STDIN`.
8. Insert an `audit.load_runs` success or failure row with the concrete
   `run_id`.

This mirrors the original Redshift load semantics while avoiding AWS.

## Batch Control

Airflow is the orchestrator, but the warehouse keeps its own batch state in
`audit.batch_runs`. This gives operators and downstream checks a stable control
record even if Airflow logs are rotated or a run is resumed manually.

State progression:

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

`FAILED` is allowed from any state. The helper script refuses accidental
backward transitions, so a stale task cannot silently move a batch from
`RAW_LOADED` back to `RAW_PREPARED`.

The local DAG starts a batch control record before source validation, marks
success milestones after each major stage, and uses an Airflow failure callback
to mark the batch `FAILED` with the failing task and error message.

## Reconciliation

After the raw PostgreSQL load, the pipeline reconciles control totals before dbt
starts. Results are written to `audit.batch_reconciliation`.

Per entity, reconciliation compares:

- expected source rows from `docs/source_profile.json` for Olist source files;
- generated manifest totals for correction feeds;
- prepared total rows;
- valid rows written to raw files;
- dead-letter rows;
- successful replay rows;
- rows currently present in the `raw` table for the `_batch_id`.

The core equations are:

```text
prepared_total_rows = expected_source_rows
prepared_valid_rows + dead_letter_rows = prepared_total_rows
raw_loaded_rows = prepared_valid_rows + replayed_rows
```

A mismatch fails the Airflow task before snapshots or marts are built. This
protects against silent data loss, duplicated loads, and incomplete file
preparation.

## Dead Letter Pattern

The project separates structural source-contract failures from record-level
failures:

- Missing files, changed headers, or changed source row counts fail fast.
- Cast/length problems in individual records are written to
  `data/raw/olist/dead_letter/...`.
- Threshold mode allows the run to continue only when both rejected row count
  and rejected row rate stay within configured bounds.
- `audit.dead_letter_events` records the rejected count, valid count, threshold
  values, dead-letter URI, and reason summary for the batch/entity.
- Corrected dead-letter rows can be replayed into raw tables without replacing
  the whole batch. The replay uses a stable `replay_id` as `_source_file`, first
  deletes any previous replay rows with the same identity, then inserts the
  corrected rows and records `audit.dead_letter_replays`.

This models a production pattern where bad messages are isolated for follow-up
without losing visibility into the successful part of the batch.

## Late-Arriving Data

The main incremental fact model uses a 3-day lookback window and also widens the
reprocess boundary when generated correction feeds contain business-effective
changes in the past.

For a run on batch date `D`, the pipeline can reprocess:

```text
D - lookback_days through D
```

plus earlier records affected by simulated SCD2 corrections.

## Backfill Strategy

The project supports:

- Initial historical load.
- Simulated daily backfills through Airflow params.
- Repeatable local demo runs with a fixed `batch_date`, even if each attempt has
  a different `run_id`.

The generated correction feeds only publish changes visible as of the selected
`batch_date`, which makes historical SCD2 behavior demonstrable even with a
static Kaggle dataset.

## AWS Design Preservation

The AWS implementation is not deleted:

- `infra/redshift/` keeps Redshift schemas, raw DDL, audit DDL, and COPY
  templates.
- `infra/aws/` keeps S3 and IAM setup notes.
- `airflow/dags/olist_modern_data_stack.py` keeps the S3 -> Redshift DAG.
- `aws-redshift-prototype` tags the pre-migration repository state.

The local version is the default because it is reproducible without cloud
account limitations.
