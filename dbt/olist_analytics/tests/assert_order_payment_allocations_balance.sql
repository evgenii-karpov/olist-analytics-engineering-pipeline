with item_orders as (
    select distinct order_id
    from {{ ref('stg_order_items') }}
),

payment_totals as (
    select
        payments.order_id,
        sum(payments.payment_value) as order_payment_value
    from {{ ref('stg_order_payments') }} as payments
    inner join item_orders
        on payments.order_id = item_orders.order_id
    group by payments.order_id
),

allocation_totals as (
    select
        order_id,
        sum(allocated_payment_value) as allocated_payment_value
    from {{ ref('int_order_payment_allocations') }}
    group by order_id
)

select
    payment_totals.order_id,
    payment_totals.order_payment_value,
    allocation_totals.allocated_payment_value,
    payment_totals.order_payment_value
    - coalesce(allocation_totals.allocated_payment_value, 0) as difference
from payment_totals
left join allocation_totals
    on payment_totals.order_id = allocation_totals.order_id
where abs(
    payment_totals.order_payment_value
    - coalesce(allocation_totals.allocated_payment_value, 0)
) > 0.10
