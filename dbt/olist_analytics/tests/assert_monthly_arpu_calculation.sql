select
    order_month,
    total_revenue,
    active_customers,
    arpu
from {{ ref('mart_monthly_arpu') }}
where
    active_customers > 0
    and abs(
        arpu - {{ round_two_decimals('total_revenue / active_customers') }}
    ) > 0.0001
