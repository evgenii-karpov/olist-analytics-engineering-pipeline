# Olist Modern Data Stack Project

This repository is a data engineering pet project built around the Olist
Brazilian e-commerce dataset. The goal is to practice a production-like Modern
Data Stack with AWS S3, Amazon Redshift, Apache Airflow, and dbt.

The project is intentionally designed for a technical interview discussion. It
focuses not only on moving data, but also on dimensional modeling, incremental
processing, SCD Type 2 history, data quality checks, late-arriving data, and
operational error handling.

## Target Architecture

```text
Olist CSV dataset
  -> Python ingestion
  -> AWS S3 raw zone
  -> Redshift raw tables via COPY
  -> dbt staging models
  -> dbt snapshots for SCD2
  -> dbt core star schema
  -> dbt marts
  -> optional Metabase dashboard later
```

## First Version Scope

1. Repo skeleton.
2. Python ingestion from `olist.zip` to S3 raw zone.
3. Redshift schemas and raw tables.
4. Airflow DAG: upload to S3, COPY into Redshift, run dbt snapshots/build/tests.
5. dbt staging models.
6. dbt snapshots for SCD2 customer and product dimensions.
7. dbt core star schema.
8. Incremental `fact_order_items` with a 3-day lookback window.
9. Marts: daily revenue and monthly ARPU.
10. README, architecture notes, decisions, and interview notes.

## Repository Layout

```text
airflow/
  dags/                 Airflow DAGs.
  include/              SQL, config, and helper files used by DAGs.

dbt/
  olist_analytics/      dbt project.

docs/
  architecture.md       End-to-end architecture and operational design.
  data_model.md         Dimensional model, grains, SCD2 strategy, marts.
  implementation_plan.md Phased delivery roadmap.
  source_contract.md    Generated source schema contract and row counts.
  source_profile.json   Machine-readable generated source profile.

infra/
  redshift/             Redshift DDL and admin SQL.
  aws/                  AWS setup notes and IAM/S3 configuration snippets.

scripts/
  ingestion/            Python ingestion scripts.
  utilities/            Local helper scripts.
```

Airflow DAG skeleton:

```text
airflow/dags/olist_modern_data_stack.py
```

dbt project:

```text
dbt/olist_analytics/
```

## Local Commands

Generate source profiling docs:

```powershell
python scripts\utilities\profile_olist_zip.py
```

Generate Redshift bootstrap SQL from the source profile:

```powershell
python scripts\utilities\generate_redshift_raw_ddl.py
```

Prepare gzip CSV files locally without uploading to S3:

```powershell
python scripts\ingestion\ingest_olist_to_s3.py --batch-date 2018-09-01 --run-id local_test_2018_09_01
```

Prepare and upload to S3 after AWS credentials are configured:

```powershell
python scripts\ingestion\ingest_olist_to_s3.py --batch-date 2018-09-01 --run-id manual_2018_09_01 --s3-bucket <bucket> --s3-prefix olist --upload
```

Generate correction feeds for SCD2 simulation:

```powershell
python scripts\ingestion\generate_correction_feeds.py --batch-date 2018-09-01 --run-id manual_2018_09_01
```

Parse the dbt project after installing requirements and configuring a profile:

```powershell
cd dbt\olist_analytics
dbt parse
```

## Main Design Decisions

- AWS S3 is the raw landing zone.
- Redshift is the warehouse.
- Airflow orchestrates ingestion, load, dbt snapshots, dbt build, and dbt tests.
- dbt owns transformations, tests, snapshots, and documentation.
- Raw data is append-only and includes metadata columns such as `_batch_id`,
  `_loaded_at`, `_source_file`, and `_source_system`.
- Staging models are views.
- Core dimensions are tables.
- SCD2 dimensions are produced from dbt snapshots.
- The main fact table is incremental with a 3-day lookback window.
- Marts are tables in the first version.

## Dataset

The local source archive is expected at:

```text
olist.zip
```

Expected CSV files:

- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`

The archive itself is ignored by git because it is a local dataset artifact.

## Interview Story

This project can be explained as a batch analytics platform for an e-commerce
business:

- Raw source files land in S3 with deterministic paths and batch metadata.
- Redshift ingests raw files with `COPY`.
- dbt turns raw data into typed staging models, then into a star schema.
- Customer and product dimensions demonstrate SCD2 history with dbt snapshots.
- Order item facts are loaded incrementally with a late-arriving data lookback.
- Data quality is enforced through dbt source, staging, core, and mart tests.
- Airflow provides orchestration, retries, failure visibility, and backfills.

## Current Status

Planning docs, repository skeleton, and source profiling are in place.
Implementation continues with the ingestion layer and AWS/Redshift setup.
