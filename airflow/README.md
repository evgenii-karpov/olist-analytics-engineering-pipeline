# Airflow

This folder contains the orchestration layer for the Olist Modern Data Stack
project.

The first DAG is a skeleton that captures the intended production flow:

```text
validate_source_contract
  -> upload_raw_files_to_s3
  -> copy_raw_files_to_redshift
  -> dbt_snapshot
  -> dbt_build
  -> dbt_test
```

The DAG is intentionally parameterized so it can support manual historical
backfills and scheduled daily runs.

## Required Environment Variables

```text
OLIST_PROJECT_ROOT
OLIST_S3_BUCKET
OLIST_S3_PREFIX
AWS_REGION
REDSHIFT_COPY_IAM_ROLE_ARN
REDSHIFT_HOST
REDSHIFT_PORT
REDSHIFT_DATABASE
REDSHIFT_USER
REDSHIFT_PASSWORD
```

`OLIST_PROJECT_ROOT` should point to the repository root. If it is not set, the
DAG attempts to infer it from the local folder layout.

## Runtime Parameters

The DAG accepts:

```text
batch_date
lookback_days
full_refresh
```

`batch_date` controls the S3 partition path and later incremental processing.
`lookback_days` is reserved for late-arriving data handling in dbt incremental
models. `full_refresh` is reserved for controlled dbt full refresh runs.
