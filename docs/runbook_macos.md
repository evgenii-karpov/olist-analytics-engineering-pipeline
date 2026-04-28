# macOS Runbook

This runbook uses shell commands from the repository root.

## Prerequisites

- Docker Desktop or another Docker runtime with the Compose plugin is running.
- `uv` is installed.

The repository includes the full `olist.zip` archive for local runs and the
committed small fixture for CI-style smoke runs.

## One-Time Setup

```bash
brew install uv
uv sync --locked
cp .env.example .env
cp dbt/olist_analytics/profiles.yml.example dbt/olist_analytics/profiles.yml
uv run pre-commit install
```

If you do not use Homebrew, install `uv` with Astral's standalone installer and
then rerun the remaining setup commands.

## Start The Local Stack

```bash
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

```bash
uv run python -m unittest discover -s tests -v
```

Run lint and formatting checks:

```bash
uv run ruff check airflow/dags scripts tests
uv run ruff format --check airflow/dags scripts tests
uv run sqlfluff lint dbt/olist_analytics/models dbt/olist_analytics/snapshots dbt/olist_analytics/tests dbt/olist_analytics/analyses dbt/olist_analytics/macros
uv run pre-commit run --all-files
```

Run the small fixture pipeline used by CI:

```bash
docker compose up -d --wait postgres airflow-postgres airflow
docker compose exec -T airflow python scripts/ci/check_fixture_pipeline_idempotency.py
```

The check resets the local analytical schemas and fixture raw directory before
the first Airflow DAG run, so use it for validation runs rather than
exploratory local tables.

## Full Manual Run

Validate the full source archive:

```bash
uv run python scripts/utilities/validate_source_contract.py
```

Prepare raw files:

```bash
uv run python scripts/ingestion/prepare_olist_raw_files.py \
  --batch-date 2018-09-01 \
  --batch-id 2018-09-01 \
  --run-id manual_2018_09_01 \
  --dead-letter-max-rows 10 \
  --dead-letter-max-rate 0.001
```

Generate correction feeds:

```bash
uv run python scripts/ingestion/generate_correction_feeds.py \
  --batch-date 2018-09-01 \
  --batch-id 2018-09-01 \
  --run-id manual_2018_09_01 \
  --dead-letter-max-rows 10 \
  --dead-letter-max-rate 0.001
```

Load raw files into PostgreSQL:

```bash
uv run python scripts/loading/load_raw_to_postgres.py \
  --bootstrap-sql-dir infra/postgres \
  --batch-date 2018-09-01 \
  --batch-id 2018-09-01 \
  --run-id manual_2018_09_01
```

Run reconciliation:

```bash
uv run python scripts/quality/reconcile_batch.py \
  --raw-dir data/raw/olist \
  --profile docs/source_profile.json \
  --bootstrap-sql-dir infra/postgres \
  --batch-date 2018-09-01 \
  --batch-id 2018-09-01 \
  --run-id manual_2018_09_01
```

Run dbt:

```bash
cd dbt/olist_analytics
export DBT_PROFILES_DIR="$PWD"
export DBT_TARGET="local_pg"
export POSTGRES_HOST="localhost"
export POSTGRES_PORT="5432"
export POSTGRES_DB="olist_analytics"
export POSTGRES_USER="olist"
export POSTGRES_PASSWORD="olist"

uv run dbt debug
uv run dbt source freshness
uv run dbt build --select staging intermediate --indirect-selection cautious --vars '{batch_date: "2018-09-01"}'
uv run dbt snapshot --vars '{batch_date: "2018-09-01"}'
uv run dbt build --exclude resource_type:snapshot --vars '{batch_date: "2018-09-01", lookback_days: 3}'
uv run dbt test --vars '{batch_date: "2018-09-01", lookback_days: 3}'
cd ../..
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

```bash
uv run python scripts/utilities/create_dead_letter_demo_archive.py
```

Run ingestion against that archive:

```bash
uv run python scripts/ingestion/prepare_olist_raw_files.py \
  --archive data/demo/dead_letter/olist_dead_letter_demo.zip \
  --output-dir data/raw/olist_dead_letter_demo \
  --batch-date 2018-09-01 \
  --batch-id 2018-09-01 \
  --run-id dead_letter_demo \
  --dead-letter-max-rows 10 \
  --dead-letter-max-rate 0.001
```

After correcting the dead-letter CSV, replay the fixed row:

```bash
uv run python scripts/loading/replay_dead_letters.py \
  --entity order_payments \
  --dead-letter-file data/raw/olist_dead_letter_demo/dead_letter/order_payments/batch_date=2018-09-01/run_id=dead_letter_demo/order_payments.csv.gz \
  --replay-id demo_payment_fix \
  --bootstrap-sql-dir infra/postgres
```

## Cleanup

Stop containers:

```bash
docker compose down
```

Remove local Docker volumes:

```bash
docker compose down -v
```
