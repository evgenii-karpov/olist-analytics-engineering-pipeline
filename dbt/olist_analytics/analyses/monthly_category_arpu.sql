select
    date_trunc('month', fact.order_purchase_timestamp)::date as order_month,
    product.product_category_name_english,
    count(distinct fact.customer_unique_id) as active_customers,
    {{
        round_two_decimals(
            'sum(coalesce(fact.allocated_payment_value, fact.gross_item_amount))'
        )
    }} as total_revenue,
    {{
        round_two_decimals(
            'sum(coalesce(fact.allocated_payment_value, fact.gross_item_amount))
            / nullif(count(distinct fact.customer_unique_id), 0)'
        )
    }} as arpu
from {{ ref('fact_order_items') }} as fact
left join {{ ref('dim_product_scd2') }} as product
    on fact.product_key = product.product_key
group by
    date_trunc('month', fact.order_purchase_timestamp)::date,
    product.product_category_name_english
order by
    order_month asc,
    total_revenue desc
