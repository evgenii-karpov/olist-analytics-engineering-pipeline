with ranked as (
    select
        product_id::varchar(256) as product_id,
        effective_at::timestamp as effective_at,
        lower(trim(product_category_name))::varchar(256) as product_category_name,
        product_weight_g::integer as product_weight_g,
        product_length_cm::integer as product_length_cm,
        product_height_cm::integer as product_height_cm,
        product_width_cm::integer as product_width_cm,
        lower(trim(change_reason))::varchar(256) as change_reason,
        _batch_id,
        _loaded_at,
        _source_file,
        _source_system,
        row_number() over (
            partition by product_id, effective_at
            order by _loaded_at desc, _batch_id desc
        ) as row_number
    from {{ source('olist', 'product_attribute_changes') }}
)

select
    product_id,
    effective_at,
    product_category_name,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    change_reason,
    _batch_id,
    _loaded_at,
    _source_file,
    _source_system
from ranked
where row_number = 1
