with expected_items as (
    select
        md5(
            order_items.order_id || '|'
            || order_items.order_item_id::varchar
        ) as order_item_key,
        order_items.order_id,
        order_items.order_item_id
    from {{ ref('stg_olist__order_items') }} as order_items
    inner join {{ ref('stg_olist__orders') }} as orders
        on order_items.order_id = orders.order_id
),

actual_items as (
    select
        order_item_key,
        order_id,
        order_item_id
    from {{ ref('fact_order_items') }}
),

missing_from_fact as (
    select
        'missing_from_fact' as issue_type,
        expected_items.order_id,
        expected_items.order_item_id
    from expected_items
    left join actual_items
        on expected_items.order_item_key = actual_items.order_item_key
    where actual_items.order_item_key is null
),

unexpected_fact_rows as (
    select
        'unexpected_fact_row' as issue_type,
        actual_items.order_id,
        actual_items.order_item_id
    from actual_items
    left join expected_items
        on actual_items.order_item_key = expected_items.order_item_key
    where expected_items.order_item_key is null
)

select *
from missing_from_fact

union all

select *
from unexpected_fact_rows
