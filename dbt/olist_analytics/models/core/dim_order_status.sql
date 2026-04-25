select
    md5(order_status) as order_status_key,
    order_status,
    coalesce(order_status = 'delivered', false) as is_successful_status,
    coalesce(order_status in ('canceled', 'unavailable'), false) as is_failed_status
from (
    select distinct order_status
    from {{ ref('stg_orders') }}
    where order_status is not null
) as statuses
