select
    customer_unique_id,
    count(*) as current_row_count
from {{ ref('dim_customer_scd2') }}
where is_current
group by customer_unique_id
having count(*) > 1
