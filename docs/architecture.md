# Architecture

## Goal

Build a production-like batch analytics pipeline for the Olist e-commerce
dataset that can run fully locally. The active stack is Python, a local
S3-shaped raw zone, PostgreSQL 18 in Docker, Apache Airflow, and dbt.

The original AWS S3 + Redshift design remains documented under `infra/redshift`
and `infra/aws`, but it is no longer required for the main development loop.

## High-Level Flow

```text
Local source archive
  -> Python ingestion
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
```

Responsibilities:

- Validate that all expected source files exist.
- Validate column names before load.
- Generate deterministic raw file paths.
- Add `_batch_id`, `_loaded_at`, `_source_file`, and `_source_system`.
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

### PostgreSQL

PostgreSQL 18 is the default local warehouse.

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

### Apache Airflow

Airflow orchestrates the local pipeline with a dedicated DAG:

```text
olist_modern_data_stack_local
```

Task flow:

```text
validate_source_contract
prepare_raw_files
generate_correction_feeds
load_raw_files_to_postgres
dbt_run_snapshot_inputs
dbt_snapshot
dbt_build
dbt_test
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

1. Delete rows for the current `_batch_id` from the target raw table.
2. Delete the matching audit row.
3. Stream the gzip CSV into PostgreSQL with `COPY FROM STDIN`.
4. Insert an `audit.load_runs` success or failure row.

This mirrors the original Redshift load semantics while avoiding AWS.

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
- Repeatable local demo runs with a fixed `batch_date` and `run_id`.

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
