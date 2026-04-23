# Airflow

This folder contains the orchestration layer for the Olist Modern Data Stack
project.

The local Docker Compose setup runs Airflow in standalone mode with SQLite and
`SequentialExecutor`. PostgreSQL is used as the analytics warehouse, not as the
Airflow metadata database.

## DAGs

Local default DAG:

```text
olist_modern_data_stack_local
```

Task flow:

```text
validate_source_contract
  -> prepare_raw_files
  -> generate_correction_feeds
  -> load_raw_files_to_postgres
  -> dbt_run_snapshot_inputs
  -> dbt_snapshot
  -> dbt_build
  -> dbt_test
```

Preserved AWS/Redshift DAG:

```text
olist_modern_data_stack
```

That DAG still models the original S3 -> Redshift COPY path and is kept for
future AWS work.

## Required Local Environment

For Docker Compose, copy `.env.example` to `.env`.

```powershell
copy .env.example .env
```

Local defaults:

```text
DBT_TARGET=local_pg
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=olist_analytics
POSTGRES_USER=olist
POSTGRES_PASSWORD=olist
```

`OLIST_PROJECT_ROOT` is set by Compose to `/opt/airflow/project`.

## Local Docker Compose

Build the local Airflow image:

```powershell
docker compose build
```

Start PostgreSQL and Airflow:

```powershell
docker compose up -d
```

Open:

```text
http://localhost:8080
```

Airflow standalone prints the generated admin password in container logs on the
first startup:

```powershell
docker compose logs airflow
```

Stop containers:

```powershell
docker compose down
```

Reset Airflow metadata:

```powershell
docker compose down
Remove-Item airflow\airflow.db -Force
```

Reset the local PostgreSQL warehouse:

```powershell
docker compose down -v
```

## VS Code and Pylance

Airflow is provided by the Docker image (`apache/airflow:2.10.5-python3.11`),
not by the local Windows virtual environment. The repository includes minimal
Pylance stubs under `typings/airflow` so VS Code can resolve DAG imports while
the real runtime dependency remains in the Airflow container.

## Runtime Parameters

Both DAGs accept:

```text
batch_date
lookback_days
full_refresh
```

`batch_date` controls raw partition paths and correction feed visibility.
The local DAG also uses `batch_date` as the stable `_batch_id` for warehouse
idempotency, while Airflow `run_id` remains the attempt identifier and raw path
partition.
`lookback_days` controls late-arriving data handling in dbt incremental models.
`full_refresh` toggles controlled dbt full refresh runs.
