# Runbook

This runbook describes the local-first end-to-end run.

## Prerequisites

- `olist.zip` exists in the repository root.
- Docker Desktop is running.
- uv is installed.
- Python dependencies are synced from `uv.lock`.
- dbt profile exists.

AWS credentials and Redshift access are not required for the default run.

## Local Setup

```powershell
winget install --id astral-sh.uv -e
uv sync --locked
copy .env.example .env
copy dbt\olist_analytics\profiles.yml.example dbt\olist_analytics\profiles.yml
```

Start PostgreSQL 18.3 and Airflow 3.2.1:

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

The analytical warehouse uses PostgreSQL 18.3. Its Compose volume is mounted at
`/var/lib/postgresql`, with `PGDATA=/var/lib/postgresql/18/docker`.
Airflow metadata uses a separate PostgreSQL 17.9 container and persists data at
`/var/lib/postgresql/data`.

## Manual Smoke Run Without Airflow

Validate the archive:

```powershell
uv run python scripts\utilities\validate_source_contract.py
```

Run fast Python tests:

```powershell
uv run python -m unittest discover -s tests -v
```

Run the small fixture integration path used by CI:

```powershell
docker compose up -d postgres
uv run python scripts\ci\run_fixture_pipeline.py --reset-warehouse
```

This validates source contract checks, raw preparation, correction feeds,
PostgreSQL load, reconciliation, dbt snapshots/build/tests, and the batch
control state machine. `--reset-warehouse` drops and recreates the local
analytical schemas, so use it for CI-style validation runs.

Prepare raw files in the local raw zone:

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

Invalid records are written to:

```text
data/raw/olist/dead_letter/<entity>/batch_date=<date>/run_id=<run_id>/<entity>.csv.gz
```

The threshold is enforced after the raw and dead-letter files are written, so a
failed run still leaves inspectable evidence.

Bootstrap PostgreSQL schemas and load raw files:

```powershell
uv run python scripts\loading\load_raw_to_postgres.py `
  --bootstrap-sql-dir infra/postgres `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
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

uv run dbt debug
uv run dbt source freshness
uv run dbt build --select staging intermediate --vars '{batch_date: "2018-09-01"}'
uv run dbt snapshot --vars '{batch_date: "2018-09-01"}'
uv run dbt build --exclude resource_type:snapshot --vars '{batch_date: "2018-09-01", lookback_days: 3}'
uv run dbt test --vars '{batch_date: "2018-09-01", lookback_days: 3}'
```

## Airflow Run

Open Airflow:

```text
http://localhost:8080
```

Local development credentials:

```text
username: admin
password: admin
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
dead_letter_max_rows: 10
dead_letter_max_rate: 0.001
```

The DAG performs:

```text
validate_source_contract
prepare_raw_files
generate_correction_feeds
load_raw_files_to_postgres
dbt_build_snapshot_inputs
dbt_snapshot
dbt_build
dbt_test
```

## Quality Gates

The run should fail if:

- Source files are missing or headers change.
- Row-level ingestion failures exceed the dead-letter threshold.
- PostgreSQL raw load fails.
- dbt source freshness fails.
- Staging/core/mart tests fail.
- SCD2 dimensions have overlapping windows.
- SCD2 dimensions have more than one current row per business key.

Inspect dead-letter audit events:

```powershell
docker compose exec -T postgres psql -U olist -d olist_analytics `
  -c "select batch_id, entity_name, failed_rows, valid_rows, reason_summary from audit.dead_letter_events order by created_at desc;"
```

Inspect batch control state:

```powershell
docker compose exec -T postgres psql -U olist -d olist_analytics `
  -c "select batch_id, orchestration_run_id, status, started_at, updated_at, finished_at from audit.batch_runs order by updated_at desc;"
```

Run reconciliation manually after raw load:

```powershell
uv run python scripts\quality\reconcile_batch.py `
  --raw-dir data/raw/olist `
  --profile docs/source_profile.json `
  --bootstrap-sql-dir infra/postgres `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
  --run-id manual_2018_09_01
```

Inspect reconciliation results:

```powershell
docker compose exec -T postgres psql -U olist -d olist_analytics `
  -c "select batch_id, entity_name, status, expected_loaded_rows, raw_loaded_rows, failed_checks from audit.batch_reconciliation order by created_at desc, entity_name;"
```

For manual runs, start or update a batch control record directly:

```powershell
uv run python scripts\orchestration\batch_control.py start `
  --bootstrap-sql-dir infra/postgres `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
  --run-id manual_2018_09_01

uv run python scripts\orchestration\batch_control.py mark `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
  --run-id manual_2018_09_01 `
  --status TESTED
```

## Dead-Letter Demo And Replay

Create a demo archive with one corrupt `payment_value` while preserving the same
source headers and row counts:

```powershell
uv run python scripts\utilities\create_dead_letter_demo_archive.py
```

Run ingestion against that archive:

```powershell
uv run python scripts\ingestion\prepare_olist_raw_files.py `
  --archive data/demo/dead_letter/olist_dead_letter_demo.zip `
  --output-dir data/raw/olist_dead_letter_demo `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
  --run-id dead_letter_demo `
  --dead-letter-max-rows 10 `
  --dead-letter-max-rate 0.001
```

The corrupt row lands in:

```text
data/raw/olist_dead_letter_demo/dead_letter/order_payments/batch_date=2018-09-01/run_id=dead_letter_demo/order_payments.csv.gz
```

After correcting the value in a dead-letter CSV, replay it into the raw table:

```powershell
uv run python scripts\loading\replay_dead_letters.py `
  --entity order_payments `
  --dead-letter-file data/raw/olist_dead_letter_demo/dead_letter/order_payments/batch_date=2018-09-01/run_id=dead_letter_demo/order_payments.csv.gz `
  --replay-id demo_payment_fix `
  --bootstrap-sql-dir infra/postgres
```

The replay is idempotent for the same `replay_id`: previous rows with the same
`_batch_id`, `_source_file`, and `_source_system` are deleted before the fixed
rows are inserted.

Inspect replay attempts:

```powershell
docker compose exec -T postgres psql -U olist -d olist_analytics `
  -c "select batch_id, entity_name, status, rows_replayed, replay_source_file from audit.dead_letter_replays order by started_at desc;"
```

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
