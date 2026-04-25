with ranked as (
    select
        order_id::varchar(256) as order_id,
        payment_sequential::integer as payment_sequential,
        lower(trim(payment_type))::varchar(64) as payment_type,
        payment_installments::integer as payment_installments,
        payment_value::decimal(18, 2) as payment_value,
        _batch_id,
        _loaded_at,
        _source_file,
        _source_system,
        row_number() over (
            partition by order_id, payment_sequential
            order by _loaded_at desc, _batch_id desc
        ) as row_number
    from {{ source('olist', 'order_payments') }}
)

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    _batch_id,
    _loaded_at,
    _source_file,
    _source_system
from ranked
where row_number = 1
