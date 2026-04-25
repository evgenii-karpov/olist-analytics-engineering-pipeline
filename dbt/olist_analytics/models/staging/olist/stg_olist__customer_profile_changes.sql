with ranked as (
    select
        customer_unique_id::varchar(256) as customer_unique_id,
        effective_at::timestamp as effective_at,
        customer_zip_code_prefix::varchar(16) as customer_zip_code_prefix,
        lower(trim(customer_city))::varchar(256) as customer_city,
        upper(trim(customer_state))::varchar(2) as customer_state,
        lower(trim(change_reason))::varchar(256) as change_reason,
        _batch_id,
        _loaded_at,
        _source_file,
        _source_system,
        row_number() over (
            partition by customer_unique_id, effective_at
            order by _loaded_at desc, _batch_id desc
        ) as row_number
    from {{ source('olist', 'customer_profile_changes') }}
)

select
    customer_unique_id,
    effective_at,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    change_reason,
    _batch_id,
    _loaded_at,
    _source_file,
    _source_system
from ranked
where row_number = 1
