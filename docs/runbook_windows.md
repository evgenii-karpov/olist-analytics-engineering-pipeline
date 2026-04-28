# Windows Runbook

This runbook uses PowerShell commands from the repository root.

## Prerequisites

- Docker Desktop is running.
- `uv` is installed.

The repository includes the full `olist.zip` archive for local runs and the
committed small fixture for CI-style smoke runs.

## One-Time Setup

```powershell
winget install --id astral-sh.uv -e
uv sync --locked
Copy-Item -Force .env.example .env
Copy-Item -Force dbt\olist_analytics\profiles.yml.example dbt\olist_analytics\profiles.yml
uv run pre-commit install
```

## Start The Local Stack

```powershell
docker compose build
docker compose up -d
```

Airflow is available at:

```text
http://localhost:8080
```

Local development credentials:

```text
username: admin
password: admin
```

## Fast Smoke Checks

Run Python tests:

```powershell
uv run python -m unittest discover -s tests -v
```

Run lint and formatting checks:

```powershell
uv run ruff check airflow\dags scripts tests
uv run ruff format --check airflow\dags scripts tests
uv run sqlfluff lint dbt\olist_analytics\models dbt\olist_analytics\snapshots dbt\olist_analytics\tests dbt\olist_analytics\analyses dbt\olist_analytics\macros
uv run pre-commit run --all-files
```

Run the small fixture pipeline used by CI:

```powershell
docker compose up -d --wait postgres airflow-postgres airflow
docker compose exec -T airflow python scripts/ci/check_fixture_pipeline_idempotency.py
```

The check resets the local analytical schemas and fixture raw directory before
the first Airflow DAG run, so use it for validation runs rather than
exploratory local tables.

## Full Manual Run

Validate the full source archive:

```powershell
uv run python scripts\utilities\validate_source_contract.py
```

Prepare raw files:

```powershell
uv run python scripts\ingestion\prepare_olist_raw_files.py `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
  --run-id manual_2018_09_01 `
  --dead-letter-max-rows 10 `
  --dead-letter-max-rate 0.001
```

Generate correction feeds:

```powershell
uv run python scripts\ingestion\generate_correction_feeds.py `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
  --run-id manual_2018_09_01 `
  --dead-letter-max-rows 10 `
  --dead-letter-max-rate 0.001
```

Load raw files into PostgreSQL:

```powershell
uv run python scripts\loading\load_raw_to_postgres.py `
  --bootstrap-sql-dir infra\postgres `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
  --run-id manual_2018_09_01
```

Run reconciliation:

```powershell
uv run python scripts\quality\reconcile_batch.py `
  --raw-dir data\raw\olist `
  --profile docs\source_profile.json `
  --bootstrap-sql-dir infra\postgres `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
  --run-id manual_2018_09_01
```

Run dbt:

```powershell
Set-Location dbt\olist_analytics
$env:DBT_PROFILES_DIR = (Get-Location).Path
$env:DBT_TARGET = "local_pg"
$env:POSTGRES_HOST = "localhost"
$env:POSTGRES_PORT = "5432"
$env:POSTGRES_DB = "olist_analytics"
$env:POSTGRES_USER = "olist"
$env:POSTGRES_PASSWORD = "olist"

uv run dbt debug
uv run dbt source freshness
uv run dbt build --select staging intermediate --indirect-selection cautious --vars '{batch_date: "2018-09-01"}'
uv run dbt snapshot --vars '{batch_date: "2018-09-01"}'
uv run dbt build --exclude resource_type:snapshot --vars '{batch_date: "2018-09-01", lookback_days: 3}'
uv run dbt test --vars '{batch_date: "2018-09-01", lookback_days: 3}'
Set-Location ..\..
```

## Airflow Run

Open the `olist_modern_data_stack_local` DAG and trigger it with the default
local parameters:

```text
batch_date: 2018-09-01
lookback_days: 3
full_refresh: false
dead_letter_max_rows: 10
dead_letter_max_rate: 0.001
```

## Dead-Letter Demo

Create a demo archive with one corrupt payment value:

```powershell
uv run python scripts\utilities\create_dead_letter_demo_archive.py
```

Run ingestion against that archive:

```powershell
uv run python scripts\ingestion\prepare_olist_raw_files.py `
  --archive data\demo\dead_letter\olist_dead_letter_demo.zip `
  --output-dir data\raw\olist_dead_letter_demo `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
  --run-id dead_letter_demo `
  --dead-letter-max-rows 10 `
  --dead-letter-max-rate 0.001
```

After correcting the dead-letter CSV, replay the fixed row:

```powershell
uv run python scripts\loading\replay_dead_letters.py `
  --entity order_payments `
  --dead-letter-file data\raw\olist_dead_letter_demo\dead_letter\order_payments\batch_date=2018-09-01\run_id=dead_letter_demo\order_payments.csv.gz `
  --replay-id demo_payment_fix `
  --bootstrap-sql-dir infra\postgres
```

## Cleanup

Stop containers:

```powershell
docker compose down
```

Remove local Docker volumes:

```powershell
docker compose down -v
```
