with base_customers_ranked as (
    select
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _loaded_at,
        row_number() over (
            partition by customer_unique_id
            order by _loaded_at desc, customer_id desc
        ) as row_number
    from {{ ref('stg_customers') }}
),

base_rows as (
    select
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        null::timestamp as latest_correction_effective_at,
        null::varchar(256) as latest_change_reason,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as dbt_valid_from,
        null::timestamp as dbt_valid_to,
        0 as source_priority
    from base_customers_ranked
    where row_number = 1
),

snapshot_rows as (
    select
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        latest_correction_effective_at,
        latest_change_reason,
        coalesce(
            latest_correction_effective_at,
            '1900-01-01'::timestamp
        ) as valid_from,
        dbt_valid_from,
        dbt_valid_to,
        1 as source_priority
    from {{ ref('snap_customers') }}
),

unioned_rows as (
    select *
    from base_rows

    union all

    select *
    from snapshot_rows
),

deduplicated_rows as (
    select
        *,
        row_number() over (
            partition by customer_unique_id, valid_from
            order by
                source_priority desc,
                case when dbt_valid_from is null then 1 else 0 end,
                dbt_valid_from desc
        ) as row_number
    from unioned_rows
),

scd2_windows as (
    select
        *,
        lead(valid_from) over (
            partition by customer_unique_id
            order by valid_from, dbt_valid_from
        ) as next_valid_from
    from deduplicated_rows
    where row_number = 1
)

select
    md5(customer_unique_id || '|' || valid_from::varchar) as customer_key,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    latest_correction_effective_at,
    latest_change_reason,
    valid_from,
    next_valid_from as valid_to,
    coalesce(next_valid_from is null, false) as is_current,
    dbt_valid_from as snapshot_valid_from,
    dbt_valid_to as snapshot_valid_to
from scd2_windows
