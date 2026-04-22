# Interview Notes

## Elevator Pitch

This project implements a production-like batch analytics platform for an
e-commerce dataset using Python, a local S3-shaped raw zone, PostgreSQL 18,
Apache Airflow, and dbt.

The first design targeted AWS S3 and Amazon Redshift. Because Redshift access
was not available, the project was adapted into a fully reproducible local
version while preserving the same architectural contracts: immutable raw files,
warehouse raw tables, dbt transformations, SCD2 dimensions, incremental facts,
and quality gates.

## What This Project Demonstrates

- Local-first data warehouse development with Docker.
- Storage contract abstraction: filesystem locally, S3 later.
- Warehouse load pattern with idempotent raw loads and audit rows.
- Orchestration with Airflow.
- dbt project structure and layered modeling.
- Source contracts and schema validation.
- Dimensional modeling and star schema design.
- SCD Type 2 dimensions with dbt snapshots.
- Business-effective SCD2 joins from fact tables.
- Incremental fact loading with a late-arriving data lookback.
- Multi-grain modeling through order-level payment allocation to item-grain
  facts.
- Data quality gates across sources, staging, core, and marts.
- Trade-off discussion between local reproducibility and cloud-native services.

## Architecture Talking Points

The raw landing zone keeps deterministic paths:

```text
data/raw/olist/raw/<entity>/batch_date=<date>/run_id=<run_id>/<entity>.csv.gz
```

That shape intentionally mirrors S3 keys. In a cloud deployment the same logical
contract can be backed by `s3://<bucket>/olist/raw/...`; locally it is backed by
the filesystem.

PostgreSQL raw tables are append-only and include metadata columns:

```text
_batch_id
_loaded_at
_source_file
_source_system
```

dbt owns the transformation logic. Staging models are views, dimensions and
marts are tables, and the main fact table is incremental.

Airflow provides the operational wrapper:

- task-level retries;
- explicit task boundaries;
- backfill parameters;
- failure visibility;
- separation between ingestion, load, snapshots, build, and test.

## Dimensional Modeling Talking Points

The central fact is `fact_order_items`.

Grain:

```text
one row per order_id + order_item_id
```

This grain was chosen because Olist order items naturally contain product,
seller, price, and freight. Payments are at order grain, so the project
allocates payment value to item grain proportionally by `price + freight_value`.

Dimensions:

- `dim_customer_scd2`
- `dim_product_scd2`
- `dim_seller`
- `dim_order_status`
- `dim_date`

Marts:

- `mart_daily_revenue`
- `mart_monthly_arpu`

## SCD2 Talking Points

Olist is a static Kaggle dataset, so the project generates deterministic
correction feeds to simulate production master-data changes.

Examples:

- customer address/profile changes;
- product category reclassification;
- product weight and size corrections.

The correction generator only publishes changes visible as of the current
`batch_date`. This makes historical backfills meaningful.

dbt snapshots use the `check` strategy. The core SCD2 dimensions expose
business-effective windows:

```text
valid_from
valid_to
is_current
snapshot_valid_from
snapshot_valid_to
```

The fact table joins to SCD2 dimensions using `order_purchase_timestamp` and the
business-effective `valid_from` / `valid_to` window.

## Incremental Loading Talking Points

`fact_order_items` is incremental and uses a late-arriving data lookback:

```text
lookback_days = 3
```

The incremental model also widens its reprocessing boundary when generated
correction feeds contain business-effective changes in the past.

## Trade-Offs

PostgreSQL is not a Redshift replacement for distributed columnar performance,
but it is a strong local warehouse stand-in for this project because it supports
schemas, transactions, `COPY`, dbt, and a familiar SQL dialect.

S3 is not required locally, but the project preserves the S3 path contract. That
keeps the codebase easy to explain and leaves a clean route back to AWS.

The AWS design is intentionally retained in `infra/redshift`, `infra/aws`, and
the preserved AWS DAG. The main branch prioritizes reproducibility.

## Future Enhancements

- Add MinIO if a local S3-compatible object store becomes useful.
- Add Metabase dashboards.
- Add CI for dbt parse/build checks.
- Store prepared raw files as Parquet.
- Add Terraform for the AWS path.
- Re-enable Redshift as an alternate deployment target when access is available.
