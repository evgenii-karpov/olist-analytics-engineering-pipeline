select
    product_id,
    valid_from,
    valid_to
from {{ ref('dim_product_scd2') }}
where
    valid_to is not null
    and valid_to <= valid_from
