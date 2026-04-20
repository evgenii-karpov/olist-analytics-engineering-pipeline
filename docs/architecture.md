# Architecture

## Goal

Build a production-like batch analytics pipeline for the Olist e-commerce
dataset using AWS S3, Amazon Redshift, Apache Airflow, and dbt.

The project should be useful both as a learning environment and as an interview
artifact. Each component should have a clear responsibility and a clear reason
to exist.

## High-Level Flow

```text
Local source archive
  -> Python ingestion
  -> S3 raw zone
  -> Redshift raw schema
  -> dbt staging schema
  -> dbt snapshots schema
  -> dbt core schema
  -> dbt marts schema
```

## Components

### Python Ingestion

Python reads `olist.zip`, extracts expected CSV files, validates their schemas,
adds operational metadata, optionally filters by a batch window, compresses the
outputs, and uploads them to S3.

The first version will use `pandas` and `boto3`.

Responsibilities:

- Validate that all expected source files exist.
- Validate column names before upload.
- Generate deterministic S3 object keys.
- Add or preserve load metadata.
- Fail early if source data is malformed.
- Support backfill windows and simulated daily batches.

### AWS S3

S3 is the raw data lake landing zone.

Proposed layout:

```text
s3://<bucket>/olist/raw/<entity>/batch_date=<YYYY-MM-DD>/run_id=<airflow_run_id>/<entity>.csv.gz
s3://<bucket>/olist/quarantine/<entity>/batch_date=<YYYY-MM-DD>/run_id=<airflow_run_id>/<entity>.csv.gz
s3://<bucket>/olist/corrections/<entity>/batch_date=<YYYY-MM-DD>/run_id=<airflow_run_id>/<entity>.csv.gz
```

Example:

```text
s3://my-bucket/olist/raw/orders/batch_date=2018-03-10/run_id=manual__2018-03-10T00:00:00/orders.csv.gz
```

### Amazon Redshift

Redshift is the analytical warehouse.

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

The first load path is `COPY` from S3 into `raw` tables. Transformations after
raw are owned by dbt.

### Apache Airflow

Airflow orchestrates the full pipeline.

Proposed DAG tasks:

```text
validate_local_dataset
generate_correction_feed
upload_raw_files_to_s3
copy_raw_files_to_redshift
run_dbt_snapshots
run_dbt_build
run_dbt_tests
record_load_status
```

Airflow provides:

- Scheduling.
- Backfills.
- Retries.
- Task-level failure visibility.
- Runtime parameters such as `batch_date`, `lookback_days`, and `full_refresh`.

### dbt

dbt owns warehouse transformations, tests, snapshots, docs, and lineage.

Layer strategy:

```text
sources      raw Redshift tables
staging      typed, renamed, lightly cleaned source models
intermediate reusable transformation models
snapshots    SCD2 history tables
core         dimensional model and star schema
marts        business-facing aggregates
```

## Redshift Schemas

### raw

Raw tables are loaded directly from S3. They preserve source fields and include
operational metadata:

```text
_batch_id
_loaded_at
_source_file
_source_system
```

Raw is append-only in the first version. Downstream dbt models are responsible
for deduplication and selecting the latest valid record where needed.

### staging

Staging models are dbt views.

They perform:

- Column renaming.
- Type casting.
- Timestamp parsing.
- Basic null handling.
- Standardized naming.
- Lightweight deduplication when needed.

### intermediate

Intermediate models hold reusable logic that is not yet business-facing.

Examples:

- Payment allocation from order-level payments to item-level facts.
- Current customer attributes after applying correction feeds.
- Current product attributes after applying correction feeds.

### snapshots

Snapshots store SCD2 history for selected dimensions:

- Customer profile attributes.
- Product attributes.

### core

Core contains the dimensional model:

- `dim_customer_scd2`
- `dim_product_scd2`
- `dim_seller`
- `dim_date`
- `dim_order_status`
- `fact_order_items`

### marts

Marts contain business-facing aggregates:

- `mart_daily_revenue`
- `mart_monthly_arpu`

## S3 To Redshift COPY

The initial approach is one Redshift raw table per source CSV.

The COPY command will load compressed CSV files from S3:

```sql
copy raw.orders
from 's3://<bucket>/olist/raw/orders/batch_date=<date>/run_id=<run_id>/'
iam_role '<redshift_iam_role_arn>'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
acceptinvchars;
```

Operational metadata can be handled in one of two ways:

- Add metadata columns to generated CSV files before upload.
- Load into temporary raw tables and add metadata during insert into final raw
  tables.

The first version will prefer adding metadata during ingestion because it keeps
the Redshift COPY path simple.

## Late-Arriving Data

The main incremental fact model will use a 3-day lookback window.

For a run on batch date `D`, the pipeline processes:

```text
D - 3 days through D
```

This handles late-arriving orders, payments, order items, reviews, or correction
events that arrive after their business event timestamp.

## Backfill Strategy

The project supports two modes:

- Initial historical load: bulk load the full Olist dataset.
- Simulated daily backfill: run the DAG across historical dates and publish only
  records visible as of each `batch_date`.

The simulated daily backfill is especially useful for demonstrating dbt
snapshots and SCD2 history.

## Error Handling

The first version should include:

- Airflow retries on network and warehouse tasks.
- Python schema validation before uploading to S3.
- Quarantine S3 path for invalid files.
- Redshift load audit records.
- dbt tests as quality gates.
- Idempotent batch handling through `_batch_id` and downstream deduplication.

The project should fail fast when source structure is invalid and fail visibly
when data quality checks break.

## Security And Cost Notes

- Do not commit AWS credentials.
- Use IAM roles for Redshift `COPY` from S3.
- Use least-privilege IAM policies for S3 access.
- Prefer small Redshift Serverless or trial configuration while learning.
- Tear down or pause warehouse resources when not in use.
