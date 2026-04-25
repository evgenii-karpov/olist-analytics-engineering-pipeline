# olist_analytics dbt Project

This dbt project transforms Olist raw tables into staging models, SCD2
dimensions, a core star schema, and business marts.

The default target is local PostgreSQL:

```text
DBT_TARGET=local_pg
```

A Redshift target is still present in `profiles.yml.example` for future AWS
work.

## Setup

Copy the profile example to this project directory or point dbt to it with
`DBT_PROFILES_DIR`.

```powershell
copy dbt\olist_analytics\profiles.yml.example dbt\olist_analytics\profiles.yml
cd dbt\olist_analytics
$env:DBT_PROFILES_DIR = (Get-Location).Path
$env:POSTGRES_HOST = "localhost"
```

## Useful Commands

```powershell
dbt debug
dbt parse
dbt source freshness
dbt build --select staging intermediate --vars '{batch_date: "2018-09-01"}'
dbt snapshot --vars '{batch_date: "2018-09-01"}'
dbt build --exclude resource_type:snapshot --vars '{batch_date: "2018-09-01", lookback_days: 3}'
dbt test --vars '{batch_date: "2018-09-01", lookback_days: 3}'
dbt compile --select batch_runs
dbt compile --select batch_reconciliation
dbt compile --select dead_letter_events
dbt compile --select dead_letter_replays
```

The project includes a schema naming macro that maps dbt custom schemas directly
to warehouse schemas such as `staging`, `intermediate`, `core`, and `marts`.

Small Postgres/Redshift SQL differences are isolated in compatibility macros
under `macros/warehouse_compat.sql`.

The `analyses/batch_runs.sql`, `analyses/batch_reconciliation.sql`,
`analyses/dead_letter_events.sql`, and `analyses/dead_letter_replays.sql`
queries are lightweight dbt entry points for reviewing batch state,
reconciliation results, rejected raw records, and replay attempts.

## Data Quality Tests

Besides schema tests, the project includes singular tests for the most important
pipeline invariants:

- `fact_order_items` must match the cleaned staging order-item grain exactly.
- Order-level payments must balance with item-level allocated payment values.
- Customer and product SCD2 windows must be positive and non-overlapping.
- Daily revenue and monthly ARPU marts must match their component formulas.
