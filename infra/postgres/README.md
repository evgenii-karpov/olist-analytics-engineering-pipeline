# Local PostgreSQL Infrastructure

PostgreSQL is the default local warehouse for the project.

The raw layer keeps the same logical contract as the AWS/S3 version: files land
under `data/raw/olist/raw/<entity>/batch_date=<date>/run_id=<run_id>/`, then are
loaded into `raw` tables with `COPY FROM STDIN`.

Run order:

```powershell
docker compose up -d postgres
python scripts\loading\load_raw_to_postgres.py --bootstrap-sql-dir infra/postgres --batch-date 2018-09-01 --run-id manual_2018_09_01
```

The Airflow local DAG runs the bootstrap and load command automatically.
