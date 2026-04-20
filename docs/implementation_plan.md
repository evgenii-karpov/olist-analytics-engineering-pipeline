# Implementation Plan

## Phase 0: Repository Skeleton

Status: complete.

Deliverables:

- Root README.
- Architecture documentation.
- Data model documentation.
- Implementation roadmap.
- Empty folders for Airflow, dbt, scripts, and infra.

## Phase 1: Local Dataset Profiling

Goal:

Understand the local Olist files before writing ingestion and warehouse DDL.

Deliverables:

- Inspect CSV columns and row counts.
- Define source schema contract.
- Define expected data types.
- Decide entity names used consistently across S3, Redshift, and dbt.

Output candidates:

- `docs/source_contract.md`
- `scripts/utilities/profile_olist_zip.py`

## Phase 2: Python Ingestion To S3

Goal:

Upload validated source files from `olist.zip` to the S3 raw zone.

Deliverables:

- Python ingestion script using `pandas` and `boto3`.
- Source file validation.
- Deterministic S3 keys.
- Gzip CSV output.
- Metadata columns:
  - `_batch_id`
  - `_loaded_at`
  - `_source_file`
  - `_source_system`
- Optional batch filtering by event date.
- Initial correction feed generation for SCD2 simulation.

Key design:

```text
batch_date
lookback_days
run_id
s3_bucket
s3_prefix
```

## Phase 3: AWS And Redshift Bootstrap

Goal:

Prepare AWS resources and Redshift schemas.

Deliverables:

- S3 bucket setup notes.
- IAM role/policy notes for Redshift COPY.
- Redshift schema creation SQL.
- Raw table DDL.
- Audit table DDL.

Output candidates:

- `infra/aws/s3_iam_setup.md`
- `infra/redshift/001_create_schemas.sql`
- `infra/redshift/002_create_raw_tables.sql`
- `infra/redshift/003_create_audit_tables.sql`

## Phase 4: Airflow DAG Skeleton

Goal:

Orchestrate ingestion, Redshift load, and dbt commands.

Deliverables:

- Airflow DAG.
- Configurable runtime parameters.
- Retries and failure handling.
- Task boundaries:
  - validate dataset
  - upload to S3
  - copy to Redshift
  - dbt snapshot
  - dbt build
  - dbt test
  - record audit status

Output candidates:

- `airflow/dags/olist_modern_data_stack.py`
- `airflow/include/redshift_copy.sql`

## Phase 5: dbt Project Bootstrap

Goal:

Create a dbt project targeting Redshift.

Deliverables:

- `dbt_project.yml`
- `profiles.yml.example`
- Source definitions.
- Staging model structure.
- Basic dbt tests.

dbt folders:

```text
models/
  staging/
  intermediate/
  core/
  marts/
snapshots/
tests/
macros/
```

## Phase 6: Staging Models

Goal:

Create typed, cleaned staging views over raw tables.

Deliverables:

- `stg_customers`
- `stg_orders`
- `stg_order_items`
- `stg_order_payments`
- `stg_order_reviews`
- `stg_products`
- `stg_sellers`
- `stg_geolocation`
- `stg_product_category_translation`
- Source and staging tests.

## Phase 7: SCD2 Snapshots

Goal:

Demonstrate Type 2 history with dbt snapshots.

Deliverables:

- Generated correction feeds.
- Intermediate current-attribute models.
- `snap_customers`
- `snap_products`
- `dim_customer_scd2`
- `dim_product_scd2`
- Tests for overlapping SCD2 windows and current rows.

## Phase 8: Core Star Schema

Goal:

Create the dimensional model.

Deliverables:

- `dim_date`
- `dim_seller`
- `dim_order_status`
- `dim_customer_scd2`
- `dim_product_scd2`
- `fact_order_items`
- Payment allocation intermediate model.
- Incremental fact with 3-day lookback.
- Core tests.

## Phase 9: Marts

Goal:

Create business-facing aggregate tables.

Deliverables:

- `mart_daily_revenue`
- `mart_monthly_arpu`
- Mart tests.
- Example analytical SQL queries.

## Phase 10: Documentation And Interview Polish

Goal:

Make the project easy to explain in an interview.

Deliverables:

- Updated README with setup and run instructions.
- Architecture diagram.
- Data model diagram.
- dbt docs generation instructions.
- Interview talking points.
- Known limitations and future improvements.

## Future Enhancements

- Add PostgreSQL as an OLTP source.
- Load Olist data into PostgreSQL.
- Add Apache NiFi.
- Implement PostgreSQL to S3 CDC flow.
- Add Metabase dashboards.
- Add data observability checks.
- Add CI for dbt parsing/tests.
- Add Terraform for AWS resources.
- Store transformed raw files as Parquet.
