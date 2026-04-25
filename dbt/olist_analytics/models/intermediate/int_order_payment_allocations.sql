with order_item_amounts as (
    select
        order_id,
        order_item_id,
        price,
        freight_value,
        price + freight_value as item_gross_amount,
        sum(price + freight_value) over (
            partition by order_id
        ) as order_gross_amount
    from {{ ref('stg_order_items') }}
),

order_payments as (
    select
        order_id,
        sum(payment_value) as order_payment_value
    from {{ ref('stg_order_payments') }}
    group by order_id
)

select
    order_item_amounts.order_id,
    order_item_amounts.order_item_id,
    order_item_amounts.item_gross_amount,
    order_item_amounts.order_gross_amount,
    order_payments.order_payment_value,
    case
        when order_item_amounts.order_gross_amount > 0
            then round(
                order_payments.order_payment_value
                * order_item_amounts.item_gross_amount
                / order_item_amounts.order_gross_amount,
                2
            )
    end as allocated_payment_value
from order_item_amounts
left join order_payments
    on order_item_amounts.order_id = order_payments.order_id
