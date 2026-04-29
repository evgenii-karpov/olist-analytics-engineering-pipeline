# Olist Modern Data Stack

Data engineering project built around the Olist Brazilian e-commerce dataset.
The default development path uses Python ingestion, a filesystem raw zone with
S3-style paths, PostgreSQL in Docker, Apache Airflow, and dbt. An AWS path is
also available with S3-backed raw files and Redshift as the warehouse target.

The repository remains reviewable without cloud access, but it is no longer
limited to local execution. You can run the default local DAG end to end or use
the AWS/Redshift DAG when cloud infrastructure is available.

## What It Demonstrates

- End-to-end batch pipeline from CSV archive to analytics marts.
- Deterministic raw-zone contract that can map to local files or object
  storage.
- Parallel Airflow DAG variants for local PostgreSQL and AWS Redshift runs.
- Row-level validation, dead-letter files, threshold checks, and replay support.
- Warehouse audit tables for batch state, raw load attempts, reconciliation,
  dead-letter events, and replays.
- Airflow orchestration with clear task boundaries and parameterized batch
  runs.
- dbt layers for staging, intermediate logic, snapshots, core dimensions/facts,
  and business marts.
- SCD Type 2 customer and product dimensions using deterministic correction
  feeds.
- Incremental fact loading with late-arriving data handling.
- Small committed fixture dataset and CI gates that exercise the real pipeline
  path quickly.

## High-Level Flow

```text
Olist CSV archive
  -> Python ingestion and validation
  -> raw and dead-letter zones on local storage or S3
  -> PostgreSQL or Redshift raw and audit schemas
  -> dbt staging, intermediate, snapshots, core, and marts
  -> Airflow-controlled quality gates
```

## Repository Layout

```text
airflow/
  dags/                 Local and AWS Airflow DAGs with separate dbt targets.

dbt/
  olist_analytics/      dbt project: sources, models, snapshots, tests,
                        analyses, macros, and profile example.

docker/
  airflow/              Airflow image and container entrypoint for local and AWS runs.

docs/
  architecture.md       System design, orchestration, audit, and reliability.
  data_model.md         Dimensional model, grains, SCD2, facts, and marts.
  ci.md                 GitHub Actions quality-gate strategy.
  diagrams.md           Mermaid architecture and data model diagrams.
  source_contract.md    Generated source-file contract from the Olist archive.
  runbook_macos.md      macOS local setup and execution commands.
  runbook_windows.md    Windows local setup and execution commands.

infra/
  postgres/             PostgreSQL warehouse DDL for schemas, raw tables, audit,
                        and correction tables.
  redshift/             Redshift warehouse DDL and COPY templates.

scripts/
  ingestion/            Source validation, raw file preparation, corrections.
  loading/              PostgreSQL/Redshift raw load and dead-letter replay.
  orchestration/        Batch-control helpers.
  quality/              Reconciliation checks.
  testing/              Fixture generation.
  utilities/            Profiling, validation, and helper scripts.

tests/
  fixtures/olist_small/ Small synthetic fixture used by CI.
  test_*.py             Python tests for ingestion, dead-letter handling,
                        replay behavior, and CI failure modes.
```

## Main Design Choices

- The local pipeline is the default path, but the AWS/Redshift path is also supported.
- Raw files are immutable and partitioned by entity, batch date, and run id.
- Structural source-contract failures fail fast, while record-level failures
  are isolated in the dead-letter zone.
- Batch lifecycle is stored in warehouse audit tables instead of relying only
  on Airflow UI state.
- Reconciliation runs before dbt so silent data loss or duplicate raw loads stop
  the pipeline early.
- dbt owns analytical modeling and data quality checks after the raw load.
- CI intentionally stays on the local PostgreSQL path so pull-request checks
  remain reproducible, self-contained, and fast.
- CI uses a small deterministic fixture while still covering the real
  ingestion, loading, reconciliation, and dbt path.

## Running Locally

Use the OS-specific runbook:

- [Windows runbook](docs/runbook_windows.md)
- [macOS runbook](docs/runbook_macos.md)

Both runbooks cover dependency setup, Docker Compose, manual smoke runs, the
Airflow DAG, dbt execution, CI-style fixture validation, and cleanup.

For AWS/Redshift execution, use the AWS DAG together with the optional
environment variables described in `.env.example`.

## Data License

The repository includes the Olist Brazilian E-Commerce Public Dataset archive
for reproducible local runs. See [Data license](DATA_LICENSE.md) for source
attribution and license terms.

## More Documentation

- [Architecture](docs/architecture.md)
- [Data model](docs/data_model.md)
- [CI quality gates](docs/ci.md)
- [Diagrams](docs/diagrams.md)
- [Source contract](docs/source_contract.md)
