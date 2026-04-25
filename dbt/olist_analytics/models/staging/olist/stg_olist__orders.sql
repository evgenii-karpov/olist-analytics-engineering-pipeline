with ranked as (
    select
        order_id::varchar(256) as order_id,
        customer_id::varchar(256) as customer_id,
        lower(trim(order_status))::varchar(64) as order_status,
        order_purchase_timestamp::timestamp as order_purchase_timestamp,
        order_approved_at::timestamp as order_approved_at,
        order_delivered_carrier_date::timestamp
            as order_delivered_carrier_date,
        order_delivered_customer_date::timestamp
            as order_delivered_customer_date,
        order_estimated_delivery_date::timestamp
            as order_estimated_delivery_date,
        _batch_id,
        _loaded_at,
        _source_file,
        _source_system,
        row_number() over (
            partition by order_id
            order by _loaded_at desc, _batch_id desc
        ) as row_number
    from {{ source('olist', 'orders') }}
)

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    _batch_id,
    _loaded_at,
    _source_file,
    _source_system
from ranked
where row_number = 1
