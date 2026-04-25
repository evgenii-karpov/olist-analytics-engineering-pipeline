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
- Decide entity names used consistently across local raw paths, optional S3,
  PostgreSQL, Redshift, and dbt.

Output candidates:

- `docs/source_contract.md`
- `docs/source_profile.json`
- `scripts/utilities/profile_olist_zip.py`

Notes:

- Source profiling reads `olist.zip` directly without extracting the archive.
- Zip-code fields are intentionally typed as `varchar` in the raw contract to
  preserve leading zeroes.
- Long review text fields use wider warehouse `varchar` types to avoid
  truncation.

## Phase 2: Python Ingestion To S3-Shaped Raw Zone

Status: complete.

Goal:

Write validated source files from `olist.zip` to the local raw zone while
preserving S3-shaped paths. Optional S3 upload remains available for the AWS
path.

Deliverables:

- Python ingestion script.
- Source file validation. Complete.
- Deterministic raw paths / S3 keys. Complete.
- Gzip CSV output. Complete.
- Metadata columns. Complete:
  - `_batch_id`
  - `_loaded_at`
  - `_source_file`
  - `_source_system`
- Optional S3 upload with `boto3`. Preserved for the AWS path.
- Optional batch filtering by event date.
- Initial correction feed generation for SCD2 simulation.

Key design:

```text
batch_date
batch_id
lookback_days
run_id
raw_dir
s3_bucket / s3_prefix for optional AWS upload
```

## Phase 3: Local PostgreSQL Bootstrap

Status: complete.

Goal:

Prepare PostgreSQL schemas and raw tables for the local warehouse.

Deliverables:

- PostgreSQL schema creation SQL. Complete.
- PostgreSQL raw table DDL. Complete.
- PostgreSQL audit table DDL. Complete.
- PostgreSQL correction table DDL. Complete.
- Python raw loader using `COPY FROM STDIN`. Complete.

Output candidates:

- `infra/postgres/001_create_schemas.sql`
- `infra/postgres/002_create_raw_tables.sql`
- `infra/postgres/003_create_audit_tables.sql`
- `scripts/loading/load_raw_to_postgres.py`

## Phase 3b: Preserved AWS And Redshift Bootstrap

Status: preserved / deferred.

The original AWS artifacts remain under `infra/aws` and `infra/redshift`.

## Phase 4: Airflow DAGs

Status: complete.

Goal:

Orchestrate ingestion, PostgreSQL load, and dbt commands locally while
preserving the original AWS DAG.

Deliverables:

- Local Airflow DAG. Complete.
- Preserved AWS Airflow DAG. Complete.
- Configurable runtime parameters. Complete.
- Retries and failure handling. Complete.
- Task boundaries:
  - validate dataset. Complete.
  - prepare local raw files. Complete.
  - load to PostgreSQL. Complete.
  - build staging/intermediate snapshot inputs. Complete.
  - dbt snapshot. Complete.
  - dbt build. Complete.
  - dbt test. Complete.
  - record audit status. Complete.

Output candidates:

- `airflow/dags/olist_modern_data_stack.py`
- `airflow/dags/olist_modern_data_stack_local.py`
- `airflow/README.md`

## Phase 5: dbt Project Bootstrap

Status: complete.

Goal:

Create a dbt project targeting local PostgreSQL by default, with Redshift kept
as an alternate target.

Deliverables:

- `dbt_project.yml`. Complete.
- `profiles.yml.example` with `local_pg` default and `redshift` target. Complete.
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
- Custom schema naming maps dbt schemas directly to warehouse schemas such as
  `staging`, `intermediate`, `core`, and `marts`.
- Generic tests include `non_negative` and `unique_combination_of_columns` so the
  project does not require `dbt_utils` in the first version.

## Phase 6: Staging Models

Goal:

Create typed, cleaned staging views over raw tables.

Deliverables:

- `stg_olist__customers`
- `stg_olist__orders`
- `stg_olist__order_items`
- `stg_olist__order_payments`
- `stg_olist__order_reviews`
- `stg_olist__products`
- `stg_olist__sellers`
- `stg_olist__geolocation`
- `stg_olist__product_category_translation`
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

Status: complete.

Goal:

Create the dimensional model.

Deliverables:

- `dim_date`. Complete.
- `dim_seller`. Complete.
- `dim_order_status`. Complete.
- `dim_customer_scd2`. Complete.
- `dim_product_scd2`. Complete.
- `fact_order_items`. Complete.
- Payment allocation intermediate model. Complete.
- Incremental fact with 3-day lookback. Complete.
- Core tests. Complete.

## Phase 9: Marts

Status: complete.

Goal:

Create business-facing aggregate tables.

Deliverables:

- `mart_daily_revenue`. Complete.
- `mart_monthly_arpu`. Complete.
- Mart tests. Complete.
- Example analytical SQL queries. Complete.

## Phase 10: Documentation And Interview Polish

Status: complete.

Goal:

Make the project easy to explain in an interview.

Deliverables:

- Updated README with setup and run instructions. Complete.
- Architecture diagram. Complete.
- Data model diagram. Complete.
- dbt docs generation instructions. Complete.
- Interview talking points. Complete.
- Known limitations and future improvements. Complete.

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
