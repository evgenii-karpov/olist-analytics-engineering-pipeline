with ordered as (
    select
        customer_unique_id,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by customer_unique_id
            order by valid_from
        ) as next_valid_from
    from {{ ref('dim_customer_scd2') }}
)

select *
from ordered
where
    valid_to is not null
    and next_valid_from is not null
    and valid_to > next_valid_from
