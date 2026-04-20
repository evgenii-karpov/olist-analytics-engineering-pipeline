select
    md5(
        customer_unique_id
        || '|' ||
        dbt_valid_from::varchar
    ) as customer_key,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    latest_correction_effective_at,
    latest_change_reason,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_customers') }}
