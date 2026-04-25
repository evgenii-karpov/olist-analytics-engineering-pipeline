{{
    config(
        materialized='incremental',
        unique_key='order_item_key',
        incremental_strategy='delete+insert'
    )
}}

{% if target.type == 'redshift' %}
    {{
        config(
            sort='order_purchase_timestamp',
            dist='order_id'
        )
    }}
{% endif %}

{% set lookback_days = var('lookback_days', 3) | int %}

-- The incremental branch references correction feeds to widen the reprocessing
-- window when SCD2 changes are business-effective in the past.
-- depends_on: {{ ref('stg_olist__customer_profile_changes') }}
-- depends_on: {{ ref('stg_olist__product_attribute_changes') }}

with

{% if is_incremental() %}

    incremental_reprocess_boundaries as (
        select
            coalesce(
            {{ dateadd_days(
                'max(order_purchase_timestamp)',
                lookback_days * -1
            ) }},
                '1900-01-01'::timestamp
            ) as reprocess_from
        from {{ this }}

        union all

        select min(effective_at) as reprocess_from
        from {{ ref('stg_olist__customer_profile_changes') }}

        union all

        select min(effective_at) as reprocess_from
        from {{ ref('stg_olist__product_attribute_changes') }}
    ),

    incremental_reprocess_window as (
        select min(reprocess_from) as reprocess_from
        from incremental_reprocess_boundaries
        where reprocess_from is not null
    ),

{% endif %}

orders as (
    select *
    from {{ ref('stg_olist__orders') }}

    {% if is_incremental() %}
        where order_purchase_timestamp >= (
            select incremental_reprocess_window.reprocess_from
            from incremental_reprocess_window
        )
    {% endif %}
),

customers as (
    select
        customer_id,
        customer_unique_id
    from {{ ref('stg_olist__customers') }}
),

payment_allocations as (
    select
        order_id,
        order_item_id,
        allocated_payment_value
    from {{ ref('int_order_payment_allocations') }}
),

customer_dim as (
    select
        customer_key,
        customer_unique_id,
        valid_from,
        valid_to
    from {{ ref('dim_customer_scd2') }}
),

product_dim as (
    select
        product_key,
        product_id,
        valid_from,
        valid_to
    from {{ ref('dim_product_scd2') }}
),

seller_dim as (
    select
        seller_key,
        seller_id
    from {{ ref('dim_seller') }}
),

order_status_dim as (
    select
        order_status_key,
        order_status
    from {{ ref('dim_order_status') }}
),

dates as (
    select
        date_key,
        date_day
    from {{ ref('dim_date') }}
),

order_items as (
    select order_items.*
    from {{ ref('stg_olist__order_items') }} as order_items
    inner join orders
        on order_items.order_id = orders.order_id
),

fact_base as (
    select
        md5(
            order_items.order_id || '|'
            || order_items.order_item_id::varchar
        ) as order_item_key,
        order_items.order_id,
        order_items.order_item_id,
        orders.customer_id,
        customers.customer_unique_id,
        order_items.product_id,
        order_items.seller_id,
        orders.order_status,
        orders.order_purchase_timestamp,
        orders.order_approved_at,
        orders.order_delivered_carrier_date,
        orders.order_delivered_customer_date,
        orders.order_estimated_delivery_date,
        order_items.shipping_limit_date,
        order_items.price,
        order_items.freight_value,
        order_items.price + order_items.freight_value as gross_item_amount,
        payment_allocations.allocated_payment_value,
        {{ days_between(
            'orders.order_purchase_timestamp',
            'orders.order_delivered_customer_date'
        ) }} as delivery_days,
        {{ days_between(
            'orders.order_estimated_delivery_date',
            'orders.order_delivered_customer_date'
        ) }} as delivery_delay_days,
        coalesce(
            orders.order_delivered_customer_date
            > orders.order_estimated_delivery_date,
            false
        ) as is_delivered_late,
        orders._batch_id,
        greatest(orders._loaded_at, order_items._loaded_at) as _loaded_at
    from order_items
    inner join orders
        on order_items.order_id = orders.order_id
    left join customers
        on orders.customer_id = customers.customer_id
    left join payment_allocations
        on
            order_items.order_id = payment_allocations.order_id
            and order_items.order_item_id = payment_allocations.order_item_id
)

select
    fact_base.order_item_key,
    fact_base.order_id,
    fact_base.order_item_id,
    customer_dim.customer_key,
    product_dim.product_key,
    seller_dim.seller_key,
    order_status_dim.order_status_key,
    purchase_date.date_key as order_purchase_date_key,
    approved_date.date_key as order_approved_date_key,
    delivered_date.date_key as order_delivered_customer_date_key,
    estimated_delivery_date.date_key as order_estimated_delivery_date_key,
    fact_base.customer_id,
    fact_base.customer_unique_id,
    fact_base.product_id,
    fact_base.seller_id,
    fact_base.order_status,
    fact_base.order_purchase_timestamp,
    fact_base.order_approved_at,
    fact_base.order_delivered_carrier_date,
    fact_base.order_delivered_customer_date,
    fact_base.order_estimated_delivery_date,
    fact_base.shipping_limit_date,
    fact_base.price,
    fact_base.freight_value,
    fact_base.gross_item_amount,
    fact_base.allocated_payment_value,
    fact_base.delivery_days,
    fact_base.delivery_delay_days,
    fact_base.is_delivered_late,
    fact_base._batch_id,
    fact_base._loaded_at
from fact_base
left join customer_dim
    on
        fact_base.customer_unique_id = customer_dim.customer_unique_id
        and fact_base.order_purchase_timestamp >= customer_dim.valid_from
        and fact_base.order_purchase_timestamp
        < coalesce(customer_dim.valid_to, '9999-12-31'::timestamp)
left join product_dim
    on
        fact_base.product_id = product_dim.product_id
        and fact_base.order_purchase_timestamp >= product_dim.valid_from
        and fact_base.order_purchase_timestamp
        < coalesce(product_dim.valid_to, '9999-12-31'::timestamp)
left join seller_dim
    on fact_base.seller_id = seller_dim.seller_id
left join order_status_dim
    on fact_base.order_status = order_status_dim.order_status
left join dates as purchase_date
    on fact_base.order_purchase_timestamp::date = purchase_date.date_day
left join dates as approved_date
    on fact_base.order_approved_at::date = approved_date.date_day
left join dates as delivered_date
    on fact_base.order_delivered_customer_date::date = delivered_date.date_day
left join dates as estimated_delivery_date
    on
        fact_base.order_estimated_delivery_date::date
        = estimated_delivery_date.date_day
