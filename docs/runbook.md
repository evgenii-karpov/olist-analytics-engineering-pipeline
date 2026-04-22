# Runbook

This runbook describes the local-first end-to-end run.

## Prerequisites

- `olist.zip` exists in the repository root.
- Docker Desktop is running.
- Python virtual environment is created.
- Python dependencies are installed.
- dbt profile exists.

AWS credentials and Redshift access are not required for the default run.

## Local Setup

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
copy dbt\olist_analytics\profiles.yml.example dbt\olist_analytics\profiles.yml
```

Start PostgreSQL 18 and Airflow:

```powershell
docker compose build
docker compose up -d
```

PostgreSQL connection defaults:

```text
host: localhost from the host machine, postgres from the Airflow container
port: 5432
database: olist_analytics
user: olist
password: olist
```

The Compose volume for PostgreSQL 18 is mounted at `/var/lib/postgresql`, with
`PGDATA=/var/lib/postgresql/18/docker`.

## Manual Smoke Run Without Airflow

Validate the archive:

```powershell
python scripts\utilities\validate_source_contract.py
```

Prepare raw files in the local raw zone:

```powershell
python scripts\ingestion\prepare_olist_raw_files.py `
  --batch-date 2018-09-01 `
  --run-id manual_2018_09_01
```

Generate correction feeds:

```powershell
python scripts\ingestion\generate_correction_feeds.py `
  --batch-date 2018-09-01 `
  --run-id manual_2018_09_01
```

Bootstrap PostgreSQL schemas and load raw files:

```powershell
python scripts\loading\load_raw_to_postgres.py `
  --bootstrap-sql-dir infra/postgres `
  --batch-date 2018-09-01 `
  --run-id manual_2018_09_01
```

## dbt Execution

```powershell
cd dbt\olist_analytics
$env:DBT_PROFILES_DIR = (Get-Location).Path
$env:DBT_TARGET = "local_pg"
$env:POSTGRES_HOST = "localhost"
$env:POSTGRES_PORT = "5432"
$env:POSTGRES_DB = "olist_analytics"
$env:POSTGRES_USER = "olist"
$env:POSTGRES_PASSWORD = "olist"

dbt debug
dbt source freshness
dbt run --select staging intermediate --vars '{batch_date: "2018-09-01"}'
dbt snapshot --vars '{batch_date: "2018-09-01"}'
dbt build --vars '{batch_date: "2018-09-01", lookback_days: 3}'
dbt test --vars '{batch_date: "2018-09-01", lookback_days: 3}'
```

## Airflow Run

Open Airflow:

```text
http://localhost:8080
```

The local DAG is:

```text
olist_modern_data_stack_local
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
prepare_raw_files
generate_correction_feeds
load_raw_files_to_postgres
dbt_run_snapshot_inputs
dbt_snapshot
dbt_build
dbt_test
```

Airflow standalone prints the generated admin password in container logs on the
first startup:

```powershell
docker compose logs airflow
```

## Quality Gates

The run should fail if:

- Source files are missing or headers change.
- PostgreSQL raw load fails.
- dbt source freshness fails.
- Staging/core/mart tests fail.
- SCD2 dimensions have overlapping windows.
- SCD2 dimensions have more than one current row per business key.

## AWS / Redshift Path

The AWS path is preserved but no longer default:

- `airflow/dags/olist_modern_data_stack.py`
- `infra/redshift/`
- `infra/aws/`
- `docs/aws_next_steps.md`

Use the `redshift` dbt profile target and the preserved AWS DAG when Redshift
access becomes available again.

## Cleanup

Stop containers:

```powershell
docker compose down
```

Remove local PostgreSQL data:

```powershell
docker compose down -v
```
