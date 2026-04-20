# olist_analytics dbt Project

This dbt project transforms Olist raw tables in Redshift into staging models,
SCD2 dimensions, a core star schema, and business marts.

## Setup

Copy the profile example to your dbt profiles directory or point dbt to this
project directory with `DBT_PROFILES_DIR`.

```powershell
copy dbt\olist_analytics\profiles.yml.example dbt\olist_analytics\profiles.yml
```

Then set Redshift environment variables from the root `.env.example`.

## Useful Commands

```powershell
dbt debug
dbt parse
dbt source freshness
dbt snapshot --vars '{batch_date: "2018-09-01"}'
dbt build --select staging
```

The project includes a schema naming macro that maps dbt custom schemas directly
to Redshift schemas such as `staging`, `intermediate`, `core`, and `marts`.
