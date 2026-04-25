with customers_ranked as (
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

base_customers as (
    select
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _loaded_at
    from customers_ranked
    where row_number = 1
),

corrections_ranked as (
    select
        customer_unique_id,
        effective_at,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        change_reason,
        _loaded_at,
        row_number() over (
            partition by customer_unique_id
            order by effective_at desc, _loaded_at desc
        ) as row_number
    from {{ ref('stg_customer_profile_changes') }}
    where effective_at <= '{{ var("batch_date", "9999-12-31") }}'::timestamp
),

latest_corrections as (
    select
        customer_unique_id,
        effective_at,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        change_reason,
        _loaded_at
    from corrections_ranked
    where row_number = 1
)

select
    base_customers.customer_unique_id,
    coalesce(latest_corrections.customer_zip_code_prefix, base_customers.customer_zip_code_prefix)
        as customer_zip_code_prefix,
    coalesce(latest_corrections.customer_city, base_customers.customer_city) as customer_city,
    coalesce(latest_corrections.customer_state, base_customers.customer_state) as customer_state,
    latest_corrections.effective_at as latest_correction_effective_at,
    latest_corrections.change_reason as latest_change_reason,
    greatest(
        base_customers._loaded_at,
        coalesce(latest_corrections._loaded_at, base_customers._loaded_at)
    ) as _loaded_at
from base_customers
left join latest_corrections
    on base_customers.customer_unique_id = latest_corrections.customer_unique_id
