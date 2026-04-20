select
    md5(
        product_id
        || '|' ||
        dbt_valid_from::varchar
    ) as product_key,
    product_id,
    product_category_name,
    product_category_name_english,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    latest_correction_effective_at,
    latest_change_reason,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_products') }}
