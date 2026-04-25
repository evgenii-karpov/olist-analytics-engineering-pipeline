with date_spine as (
    select distinct order_purchase_timestamp::date as date_day
    from {{ ref('stg_olist__orders') }}
    where order_purchase_timestamp is not null

    union

    select distinct order_approved_at::date as date_day
    from {{ ref('stg_olist__orders') }}
    where order_approved_at is not null

    union

    select distinct order_delivered_carrier_date::date as date_day
    from {{ ref('stg_olist__orders') }}
    where order_delivered_carrier_date is not null

    union

    select distinct order_delivered_customer_date::date as date_day
    from {{ ref('stg_olist__orders') }}
    where order_delivered_customer_date is not null

    union

    select distinct order_estimated_delivery_date::date as date_day
    from {{ ref('stg_olist__orders') }}
    where order_estimated_delivery_date is not null
)

select
    to_char(date_day, 'YYYYMMDD')::integer as date_key,
    date_day,
    extract(year from date_day)::integer as year_number,
    extract(month from date_day)::integer as month_number,
    extract(day from date_day)::integer as day_number,
    extract(quarter from date_day)::integer as quarter_number,
    extract(week from date_day)::integer as week_number,
    extract(dow from date_day)::integer as day_of_week_number,
    to_char(date_day, 'YYYY-MM') as year_month,
    to_char(date_day, 'Month') as month_name,
    coalesce(extract(dow from date_day) in (0, 6), false) as is_weekend
from date_spine
