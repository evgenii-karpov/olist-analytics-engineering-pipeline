# CI Quality Gates

The GitHub Actions workflow is intentionally split into small jobs. Each job
guards a different failure class, so a failing check points to a useful layer
instead of becoming one large opaque pipeline failure.

## Workflow

```text
python-unit
  -> Python syntax, source contract fixture validation, unit tests,
     and targeted negative data-quality tests.

dbt-static
  -> dbt parse without a warehouse connection.

airflow-imports
  -> docker compose config, Airflow image build, and isolated DagBag imports.

fixture-integration
  -> PostgreSQL 18 service container and the small fixture end-to-end path.
```

## Small Fixture Dataset

The committed fixture lives in `tests/fixtures/olist_small`.

It contains:

- `olist_small.zip`, with the original Olist file names and headers.
- `source_profile_small.json`, the matching source contract.
- `source/`, reviewable uncompressed CSVs.

The fixture is synthetic, small, and referentially consistent. It is designed
to exercise real joins, correction feed generation, reconciliation, dbt
snapshots, core models, marts, and tests without downloading the full Kaggle
archive in CI.

Regenerate it when needed:

```powershell
.\.venv\Scripts\python.exe scripts\testing\create_small_fixture_dataset.py
```

## Local CI Commands

Fast checks:

```powershell
.\.venv\Scripts\python.exe -m compileall airflow\dags scripts tests
.\.venv\Scripts\python.exe scripts\utilities\validate_source_contract.py `
  --archive tests\fixtures\olist_small\olist_small.zip `
  --profile tests\fixtures\olist_small\source_profile_small.json
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

dbt static parse:

```powershell
cd dbt\olist_analytics
..\..\.venv\Scripts\dbt.exe parse --no-partial-parse --show-all-deprecations
```

Small fixture integration:

```powershell
docker compose up -d postgres
.\.venv\Scripts\python.exe scripts\ci\run_fixture_pipeline.py --reset-warehouse
```

`--reset-warehouse` drops and recreates the local analytical schemas
(`raw`, `audit`, `staging`, `intermediate`, `snapshots`, `core`, `marts`). Use
it for CI-style runs, not when you want to preserve exploratory local tables.

## What CI Tests

The PR-level workflow checks the main happy path and selected failure modes.

Happy path:

- source contract validation against the small fixture archive;
- raw file preparation with row-level validation;
- generated correction feeds;
- PostgreSQL raw load;
- batch control state transitions;
- source-to-raw reconciliation;
- dbt staging/intermediate build;
- dbt snapshots;
- dbt core and mart build;
- dbt tests.

Failure modes:

- source contract failure when a required column is missing;
- corrupt source row being routed to the dead-letter path;
- dead-letter threshold failure;
- reconciliation gate failure.

The full `olist.zip` run remains a local/manual validation path. CI favors a
small, deterministic confidence gate so pull requests stay fast and reliable.
