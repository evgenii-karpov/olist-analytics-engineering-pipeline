# CI Quality Gates

The GitHub Actions workflow is split into focused jobs so a failing check points
to a useful layer instead of one opaque pipeline failure.

## Workflow

```text
lint
  -> Ruff, SQLFluff, and pre-commit checks.

python-unit
  -> Python syntax, source-contract fixture validation, unit tests,
     and targeted negative data-quality tests.

dbt-static
  -> dbt parse without a warehouse connection.

airflow-imports
  -> Docker Compose validation, Airflow image build, metadata database startup,
     and isolated DAG imports.

fixture-integration
  -> Small fixture end-to-end path through PostgreSQL, reconciliation, dbt
     snapshots/build/tests, batch-control checks, and incremental replay
     idempotency.
```

## Small Fixture Dataset

The committed fixture lives in `tests/fixtures/olist_small`.

It contains:

- `olist_small.zip`, with the original Olist file names and headers.
- `source_profile_small.json`, the matching source contract.
- `source/`, reviewable uncompressed CSVs.

The fixture is synthetic, small, and referentially consistent. It exercises real
joins, correction feed generation, reconciliation, dbt snapshots, core models,
marts, and tests without requiring the full Kaggle archive in CI.

CI uses `DEFAULT_FIXTURE_BATCH_DATE` (`2018-09-01`) as the default fixture
batch date. This date is intentionally after all generated customer/product
correction `effective_at` values, so one fixture run sees the complete
synthetic SCD2 scenario instead of needing a multi-batch backfill sequence.

## What CI Tests

Happy path:

- source contract validation against the small fixture archive;
- raw file preparation with row-level validation;
- generated correction feeds;
- PostgreSQL raw load;
- batch control state transitions;
- source-to-raw reconciliation;
- dbt staging and intermediate build;
- dbt snapshots;
- dbt core and mart build;
- dbt tests.
- incremental replay of the same fixture batch with stable analytical output
  fingerprints.

Failure modes:

- source contract failure when a required column is missing;
- corrupt source row being routed to the dead-letter path;
- dead-letter threshold failure;
- reconciliation gate failure.

The full `olist.zip` run remains a local/manual validation path. Use the
[Windows runbook](runbook_windows.md) or [macOS runbook](runbook_macos.md) for
the concrete local commands.
