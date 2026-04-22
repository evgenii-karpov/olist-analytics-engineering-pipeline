# AWS Next Steps

Status: deferred. The active `main` branch now runs locally with PostgreSQL.
This checklist is preserved for a future AWS S3 + Redshift deployment.

Use this checklist when moving from local development to the first real
end-to-end AWS run.

## Decisions Needed

Choose:

- AWS region.
- S3 bucket name.
- Redshift Serverless or provisioned cluster.
- Redshift database name.
- Redshift admin/user credentials.

Recommended starting point:

```text
Redshift Serverless
Small capacity setting
One project S3 bucket
One project IAM role for COPY
```

## Values To Collect

```text
AWS_REGION
OLIST_S3_BUCKET
OLIST_S3_PREFIX
REDSHIFT_COPY_IAM_ROLE_ARN
REDSHIFT_HOST
REDSHIFT_PORT
REDSHIFT_DATABASE
REDSHIFT_USER
REDSHIFT_PASSWORD
```

## AWS Setup Checklist

1. Create S3 bucket.
2. Create IAM role for Redshift COPY.
3. Attach S3 read policy to the role.
4. Attach the role to Redshift.
5. Create or start Redshift.
6. Confirm local AWS credentials work.
7. Run Redshift bootstrap SQL.
8. Upload raw files and correction feeds.
9. Run COPY into raw tables.
10. Run dbt snapshots/build/tests.
11. Trigger Airflow DAG.

## Smoke Checks

After S3 upload:

```text
s3://<bucket>/olist/raw/orders/batch_date=2018-09-01/run_id=<run_id>/orders.csv.gz
s3://<bucket>/olist/raw/customer_profile_changes/batch_date=2018-09-01/run_id=<run_id>/customer_profile_changes.csv.gz
```

After Redshift COPY:

```sql
select count(*) from raw.orders;
select count(*) from raw.order_items;
select count(*) from raw.customer_profile_changes;
select count(*) from audit.load_runs;
```

After dbt:

```sql
select count(*) from core.fact_order_items;
select count(*) from core.dim_customer_scd2 where not is_current;
select count(*) from core.dim_product_scd2 where not is_current;
select * from marts.mart_daily_revenue order by order_purchase_date limit 10;
select * from marts.mart_monthly_arpu order by order_month limit 10;
```

## Cost Controls

- Pause or stop Redshift when done.
- Keep S3 data under `s3://<bucket>/olist/`.
- Delete test prefixes that are not needed.
- Avoid repeated full backfills while Redshift is running.
