# Known Limitations

## Static Source Dataset

Olist is a static Kaggle dataset. The project simulates change data through
deterministic correction feeds so SCD2 behavior can be demonstrated.

This is acceptable for the first version, but a real production system would
receive changes from operational systems, CDC, or master-data feeds.

## Local PostgreSQL Is Not Redshift

PostgreSQL is the default local warehouse because it is reproducible and easy to
run in Docker. It does not demonstrate Redshift-specific distributed storage,
columnar execution, workload management, sort keys, distribution keys, or S3
`COPY` performance.

The Redshift design is preserved under `infra/redshift` and can be revived when
Redshift access is available.

## No Terraform Yet

AWS infrastructure is documented but not provisioned as code. Terraform is a
good future enhancement after the local path is stable.

## Local Airflow Uses SQLite

The project includes a Docker Compose Airflow runtime, but it intentionally uses
SQLite and `SequentialExecutor` for local testing. This is fine for a pet
project, but a production Airflow deployment should use a metadata database such
as Postgres and a production executor.

## SCD2 Correction Feed Size

The correction feed generates a small deterministic set of customer and product
changes. This is enough to demonstrate the pattern. A future version could
generate larger and more varied correction streams.

## CSV Raw Format

The first local version uses CSV.GZ to keep loading easy to inspect and debug.
Parquet could be added later for stronger typing and better compression.

## No Dashboard Yet

Metabase is intentionally deferred. The current version focuses on ingestion,
warehouse loading, dbt modeling, and quality checks.
