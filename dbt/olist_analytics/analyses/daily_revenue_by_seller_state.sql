select
    fact.order_purchase_timestamp::date as order_purchase_date,
    seller.seller_state,
    {{ round_two_decimals('sum(fact.gross_item_amount)') }} as gross_revenue,
    count(distinct fact.order_id) as orders_count
from {{ ref('fact_order_items') }} as fact
left join {{ ref('dim_seller') }} as seller
    on fact.seller_key = seller.seller_key
group by
    fact.order_purchase_timestamp::date,
    seller.seller_state
order by
    order_purchase_date asc,
    gross_revenue desc
