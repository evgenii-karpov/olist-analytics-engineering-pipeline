# Runbook

This runbook describes the intended first production-like end-to-end run.

## Prerequisites

- `olist.zip` exists in the repository root.
- Python virtual environment is created.
- Python dependencies are installed.
- dbt profile exists.
- AWS credentials are configured locally.
- S3 bucket exists.
- Redshift cluster or serverless workgroup exists.
- Redshift has an IAM role that can read the project S3 prefix.

## Local Setup

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Copy the dbt profile example:

```powershell
copy dbt\olist_analytics\profiles.yml.example dbt\olist_analytics\profiles.yml
```

For Docker Compose and Airflow, copy the environment template and fill it with
your local AWS/Redshift values:

```powershell
copy .env.example .env
```

Keep `.env` private. It is ignored by git and is loaded into the Airflow
container at startup without exposing secrets or infrastructure identifiers
through Compose config.

For local dbt commands:

```powershell
cd dbt\olist_analytics
$env:DBT_PROFILES_DIR = (Get-Location).Path
dbt parse --show-all-deprecations --no-partial-parse
```

## AWS Environment Variables

Set these before an end-to-end run:

```powershell
$env:AWS_PROFILE = "<profile>"
$env:AWS_REGION = "<region>"
$env:OLIST_S3_BUCKET = "<bucket>"
$env:OLIST_S3_PREFIX = "olist"
$env:REDSHIFT_COPY_IAM_ROLE_ARN = "<role-arn>"
$env:REDSHIFT_HOST = "<redshift-endpoint>"
$env:REDSHIFT_PORT = "5439"
$env:REDSHIFT_DATABASE = "<database>"
$env:REDSHIFT_USER = "<user>"
$env:REDSHIFT_PASSWORD = "<password>"
```

## Bootstrap Redshift

Run these SQL files in order:

```text
infra/redshift/001_create_schemas.sql
infra/redshift/002_create_raw_tables.sql
infra/redshift/003_create_audit_tables.sql
infra/redshift/005_create_correction_tables.sql
```

`004_copy_raw_tables_template.sql` is a human-readable COPY template. The
Airflow DAG performs the COPY commands programmatically.

## Manual Smoke Run Without Airflow

Validate that the local archive still matches the committed source contract:

```powershell
python scripts\utilities\validate_source_contract.py
```

Prepare and upload raw Olist files:

```powershell
python scripts\ingestion\ingest_olist_to_s3.py `
  --batch-date 2018-09-01 `
  --run-id manual_2018_09_01 `
  --s3-bucket $env:OLIST_S3_BUCKET `
  --s3-prefix $env:OLIST_S3_PREFIX `
  --upload
```

Generate and upload correction feeds:

```powershell
python scripts\ingestion\generate_correction_feeds.py `
  --batch-date 2018-09-01 `
  --run-id manual_2018_09_01 `
  --s3-bucket $env:OLIST_S3_BUCKET `
  --s3-prefix $env:OLIST_S3_PREFIX `
  --upload
```

Then run the Redshift COPY statements or use the Airflow DAG task.

## dbt Execution Order

After raw tables are loaded:

```powershell
cd dbt\olist_analytics
$env:DBT_PROFILES_DIR = (Get-Location).Path

dbt source freshness
dbt snapshot --vars '{batch_date: "2018-09-01"}'
dbt build --vars '{batch_date: "2018-09-01", lookback_days: 3}'
```

## Airflow Run

The intended DAG is:

```text
olist_modern_data_stack
```

Runtime params:

```text
batch_date: 2018-09-01
lookback_days: 3
full_refresh: false
```

The DAG performs:

```text
validate_source_contract
upload_raw_files_to_s3
generate_and_upload_correction_feeds
copy_raw_files_to_redshift
dbt_snapshot
dbt_build
dbt_test
```

## Quality Gates

The run should fail if:

- Source files are missing or headers change.
- Redshift COPY fails.
- dbt source freshness fails.
- Staging/core/mart tests fail.
- SCD2 dimensions have overlapping windows.
- SCD2 dimensions have more than one current row per business key.

## Cleanup

For cost control:

- Stop or pause Redshift compute when not in use.
- Keep S3 data under one project prefix.
- Remove experimental S3 prefixes after testing.
