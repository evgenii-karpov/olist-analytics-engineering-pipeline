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

Status: complete.

Goal:

Understand the local Olist files before writing ingestion and warehouse DDL.

Deliverables:

- Inspect CSV columns and row counts.
- Define source schema contract.
- Define expected data types.
- Decide entity names used consistently across S3, Redshift, and dbt.

Output candidates:

- `docs/source_contract.md`
- `docs/source_profile.json`
- `scripts/utilities/profile_olist_zip.py`

Notes:

- Source profiling reads `olist.zip` directly without extracting the archive.
- Zip-code fields are intentionally typed as `varchar` in the raw contract to
  preserve leading zeroes.
- Long review text fields use wider Redshift `varchar` types to avoid
  truncation.

## Phase 2: Python Ingestion To S3

Status: complete.

Goal:

Upload validated source files from `olist.zip` to the S3 raw zone.

Deliverables:

- Python ingestion script.
- Source file validation. Complete.
- Deterministic S3 keys. Complete.
- Gzip CSV output. Complete.
- Metadata columns. Complete:
  - `_batch_id`
  - `_loaded_at`
  - `_source_file`
  - `_source_system`
- Optional S3 upload with `boto3`. Implemented, not yet tested against AWS.
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

Status: in progress.

Goal:

Prepare AWS resources and Redshift schemas.

Deliverables:

- S3 bucket setup notes. Complete.
- IAM role/policy notes for Redshift COPY. Complete.
- Redshift schema creation SQL. Complete.
- Raw table DDL. Complete.
- Audit table DDL. Complete.
- Redshift COPY template. Complete.

Output candidates:

- `infra/aws/s3_iam_setup.md`
- `infra/redshift/001_create_schemas.sql`
- `infra/redshift/002_create_raw_tables.sql`
- `infra/redshift/003_create_audit_tables.sql`

## Phase 4: Airflow DAG Skeleton

Status: in progress.

Goal:

Orchestrate ingestion, Redshift load, and dbt commands.

Deliverables:

- Airflow DAG. Initial skeleton complete.
- Configurable runtime parameters. Initial skeleton complete.
- Retries and failure handling. Initial skeleton complete.
- Task boundaries:
  - validate dataset. Complete.
  - upload to S3. Complete.
  - copy to Redshift. Complete.
  - dbt snapshot. Complete.
  - dbt build. Complete.
  - dbt test. Complete.
  - record audit status

Output candidates:

- `airflow/dags/olist_modern_data_stack.py`
- `airflow/README.md`

## Phase 5: dbt Project Bootstrap

Status: complete.

Goal:

Create a dbt project targeting Redshift.

Deliverables:

- `dbt_project.yml`. Complete.
- `profiles.yml.example`. Complete.
- Source definitions. Complete.
- Staging model structure. Complete.
- Basic dbt tests. Complete.

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

Notes:

- Staging models are implemented as views.
- Custom schema naming maps dbt schemas directly to Redshift schemas such as
  `staging`, `intermediate`, `core`, and `marts`.
- Generic tests include `non_negative` and `unique_combination_of_columns` so the
  project does not require `dbt_utils` in the first version.

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

Status: complete.

Goal:

Demonstrate Type 2 history with dbt snapshots.

Deliverables:

- Generated correction feeds. Complete.
- Raw correction tables. Complete.
- Staging correction models. Complete.
- Intermediate current-attribute models. Complete.
- `snap_customers`. Complete.
- `snap_products`. Complete.
- `dim_customer_scd2`. Complete.
- `dim_product_scd2`. Complete.
- Tests for overlapping SCD2 windows and current rows. Complete.

Notes:

- Correction feeds simulate production master-data updates over a static Kaggle
  dataset.
- Intermediate models use the dbt `batch_date` variable to expose only
  corrections that should be visible in a given batch.
- dbt snapshots track customer and product changes using the `check` strategy.

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
