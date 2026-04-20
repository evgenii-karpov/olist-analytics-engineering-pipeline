select
    product_id,
    count(*) as current_row_count
from {{ ref('dim_product_scd2') }}
where is_current
group by product_id
having count(*) > 1
