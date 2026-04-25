# Olist Modern Data Stack Project

This repository is a data engineering pet project built around the Olist
Brazilian e-commerce dataset. The current version is local-first: it runs with
Python ingestion, a local S3-shaped raw zone, PostgreSQL 18 in Docker, Apache
Airflow, and dbt.

The original AWS S3 + Amazon Redshift design is preserved under `infra/redshift`
and the git tag `aws-redshift-prototype`. The active `main` branch is designed
to be runnable without AWS access.

## Target Architecture

```text
Olist CSV dataset
  -> Python ingestion
  -> row-level validation + dead-letter files
  -> local raw zone shaped like S3 keys
  -> PostgreSQL raw tables via COPY FROM STDIN
  -> dbt staging models
  -> dbt snapshots for SCD2
  -> dbt core star schema
  -> dbt marts
  -> optional Metabase dashboard later
```

The local raw zone mirrors the old S3 contract:

```text
data/raw/olist/raw/<entity>/batch_date=<YYYY-MM-DD>/run_id=<run_id>/<entity>.csv.gz
data/raw/olist/dead_letter/<entity>/batch_date=<YYYY-MM-DD>/run_id=<run_id>/<entity>.csv.gz
```

This keeps the production conversation intact: in AWS, the same logical paths
can be backed by S3; locally, they are backed by the filesystem.

## What This Project Demonstrates

- Reproducible local data warehouse workflow with Docker.
- Raw landing-zone contract independent of a specific storage backend.
- Batch control state machine persisted in the warehouse audit schema.
- Reconciliation of source, prepared, dead-letter, replay, and raw load counts.
- Dead Letter Pattern for record-level ingestion failures with threshold mode.
- Small fixture dataset and GitHub Actions quality gates for CI confidence.
- PostgreSQL warehouse loading with `COPY FROM STDIN`.
- Airflow orchestration with retries, params, and task-level visibility.
- dbt layered modeling: staging, intermediate, snapshots, core, marts.
- SCD Type 2 customer and product dimensions with generated correction feeds.
- Incremental fact loading with a late-arriving data lookback.
- Data quality checks across sources, staging, core, and marts.
- Interview-ready trade-off discussion: local reproducibility vs cloud-native
  S3/Redshift deployment.

## Repository Layout

```text
airflow/
  dags/                 Airflow DAGs. Local and AWS DAGs are separate.

dbt/
  olist_analytics/      dbt project.

docs/
  architecture.md       End-to-end architecture and operational design.
  ci.md                 GitHub Actions checks and fixture pipeline strategy.
  data_model.md         Dimensional model, grains, SCD2 strategy, marts.
  diagrams.md           Mermaid architecture and data model diagrams.
  runbook.md            Local run instructions.
  interview_notes.md    Talking points for technical interviews.
  aws_next_steps.md     Deferred AWS checklist.

infra/
  postgres/             Local PostgreSQL DDL.
  redshift/             Preserved Redshift DDL and admin SQL.
  aws/                  Preserved AWS setup notes.

scripts/
  ingestion/            Raw file preparation and correction feed generation.
  loading/              PostgreSQL raw loading scripts.
  utilities/            Local helper scripts.
```

## Local Commands

Install uv, create the Python 3.11 environment, and install locked project
dependencies:

```powershell
winget install --id astral-sh.uv -e
uv sync --locked
```

Python dependencies are declared in `pyproject.toml` and locked in `uv.lock`.
After editing dependencies, refresh the lock file with:

```powershell
uv lock
```

Install the pre-commit hooks once per clone:

```powershell
uv run pre-commit install
```

Start PostgreSQL 18 and Airflow:

```powershell
copy .env.example .env
docker compose build
docker compose up -d
```

PostgreSQL uses the official `postgres:18` image. Because PostgreSQL 18 changed
the image data layout, Compose mounts the named volume at `/var/lib/postgresql`
and sets `PGDATA=/var/lib/postgresql/18/docker`.

Validate the local archive:

```powershell
uv run python scripts\utilities\validate_source_contract.py
```

Run fast Python tests for ingestion, dead-letter, and replay logic:

```powershell
uv run python -m unittest discover -s tests -v
```

Run Python linting and format checks:

```powershell
uv run ruff check airflow\dags scripts tests
uv run ruff format --check airflow\dags scripts tests
uv run pre-commit run --all-files
```

Run dbt SQL linting:

```powershell
Copy-Item -Force dbt\olist_analytics\profiles.yml.example dbt\olist_analytics\profiles.yml
uv run sqlfluff lint dbt\olist_analytics\models dbt\olist_analytics\snapshots dbt\olist_analytics\tests dbt\olist_analytics\analyses dbt\olist_analytics\macros
```

Run the small fixture pipeline used by CI:

```powershell
docker compose up -d postgres
uv run python scripts\ci\run_fixture_pipeline.py --reset-warehouse
```

`--reset-warehouse` drops and recreates the local analytical schemas, so use it
for CI-style validation runs rather than ad hoc exploration.

Prepare local raw files:

```powershell
uv run python scripts\ingestion\prepare_olist_raw_files.py --batch-date 2018-09-01 --batch-id 2018-09-01 --run-id manual_2018_09_01
uv run python scripts\ingestion\generate_correction_feeds.py --batch-date 2018-09-01 --batch-id 2018-09-01 --run-id manual_2018_09_01
```

Both ingestion commands run row-level validation before warehouse load. Invalid
records are written to the dead-letter zone and the run continues only while
the rejected rows stay within the configured threshold
(`--dead-letter-max-rows`, `--dead-letter-max-rate`).

For a reproducible DLQ demo, create a copy of the archive with one corrupt
record, run ingestion against it, fix the dead-letter CSV, and replay the fixed
row:

```powershell
uv run python scripts\utilities\create_dead_letter_demo_archive.py
uv run python scripts\loading\replay_dead_letters.py --entity order_payments --dead-letter-file <fixed_dead_letter_csv_gz> --replay-id demo_payment_fix
```

Load raw files into PostgreSQL:

```powershell
uv run python scripts\loading\load_raw_to_postgres.py `
  --bootstrap-sql-dir infra/postgres `
  --batch-date 2018-09-01 `
  --batch-id 2018-09-01 `
  --run-id manual_2018_09_01
```

Run dbt locally:

```powershell
copy dbt\olist_analytics\profiles.yml.example dbt\olist_analytics\profiles.yml
cd dbt\olist_analytics
$env:DBT_PROFILES_DIR = (Get-Location).Path
$env:POSTGRES_HOST = "localhost"
uv run dbt debug
uv run dbt source freshness
uv run dbt run --select staging intermediate --vars '{batch_date: "2018-09-01"}'
uv run dbt snapshot --vars '{batch_date: "2018-09-01"}'
uv run dbt build --exclude resource_type:snapshot --vars '{batch_date: "2018-09-01", lookback_days: 3}'
uv run dbt test --vars '{batch_date: "2018-09-01", lookback_days: 3}'
```

## Airflow DAGs

Local default DAG:

```text
airflow/dags/olist_modern_data_stack_local.py
```

Preserved AWS/Redshift DAG:

```text
airflow/dags/olist_modern_data_stack.py
```

The local DAG performs:

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

## Main Design Decisions

- PostgreSQL 18 in Docker is the default local warehouse.
- The raw zone is local filesystem storage, but keeps S3-style deterministic
  paths.
- Record-level ingestion failures are isolated in the dead-letter zone; source
  contract failures such as missing files or changed headers still fail fast.
- S3/Redshift artifacts are retained as an alternate AWS design, not the active
  default path.
- dbt targets PostgreSQL by default and keeps a Redshift profile target.
- Small SQL dialect differences are isolated in dbt macros where practical.
- Raw data includes stable logical `_batch_id`, `_loaded_at`,
  `_source_file`, and `_source_system`.
- Dead-letter events are audited in `audit.dead_letter_events` alongside raw
  load attempts in `audit.load_runs`.
- Corrected dead-letter rows can be replayed idempotently and audited in
  `audit.dead_letter_replays`.
- Batch lifecycle is tracked in `audit.batch_runs`, independently of Airflow UI
  state.
- Raw load control totals are tracked in `audit.batch_reconciliation` and fail
  the DAG before dbt if counts drift.
- CI uses a committed small fixture for fast pull request checks and keeps the
  full `olist.zip` path as a local/manual validation path.
- Staging models are views; core dimensions, facts, and marts are tables.

## Current Status

The project is now designed for a local end-to-end run with Docker, PostgreSQL,
Airflow, and dbt. AWS S3 + Redshift remains documented for future use when
Redshift access is available again.

Useful docs:

- [Architecture](docs/architecture.md)
- [CI Quality Gates](docs/ci.md)
- [Data Model](docs/data_model.md)
- [Diagrams](docs/diagrams.md)
- [Runbook](docs/runbook.md)
- [Interview Notes](docs/interview_notes.md)
- [AWS Next Steps](docs/aws_next_steps.md)
- [Known Limitations](docs/known_limitations.md)
