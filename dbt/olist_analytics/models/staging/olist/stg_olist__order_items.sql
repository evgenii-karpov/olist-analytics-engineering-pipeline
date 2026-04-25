with ranked as (
    select
        order_id::varchar(256) as order_id,
        order_item_id::integer as order_item_id,
        product_id::varchar(256) as product_id,
        seller_id::varchar(256) as seller_id,
        shipping_limit_date::timestamp as shipping_limit_date,
        price::decimal(18, 2) as price,
        freight_value::decimal(18, 2) as freight_value,
        _batch_id,
        _loaded_at,
        _source_file,
        _source_system,
        row_number() over (
            partition by order_id, order_item_id
            order by _loaded_at desc, _batch_id desc
        ) as row_number
    from {{ source('olist', 'order_items') }}
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    _batch_id,
    _loaded_at,
    _source_file,
    _source_system
from ranked
where row_number = 1
