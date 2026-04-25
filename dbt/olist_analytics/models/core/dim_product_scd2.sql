with base_rows as (
    select
        products.product_id,
        products.product_category_name,
        translations.product_category_name_english,
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm,
        null::timestamp as latest_correction_effective_at,
        null::varchar(256) as latest_change_reason,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as dbt_valid_from,
        null::timestamp as dbt_valid_to,
        0 as source_priority
    from {{ ref('stg_products') }} as products
    left join {{ ref('stg_product_category_translation') }} as translations
        on products.product_category_name = translations.product_category_name
),

snapshot_rows as (
    select
        product_id,
        product_category_name,
        product_category_name_english,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        latest_correction_effective_at,
        latest_change_reason,
        coalesce(
            latest_correction_effective_at,
            '1900-01-01'::timestamp
        ) as valid_from,
        dbt_valid_from,
        dbt_valid_to,
        1 as source_priority
    from {{ ref('snap_products') }}
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
            partition by product_id, valid_from
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
            partition by product_id
            order by valid_from, dbt_valid_from
        ) as next_valid_from
    from deduplicated_rows
    where row_number = 1
)

select
    md5(product_id || '|' || valid_from::varchar) as product_key,
    product_id,
    product_category_name,
    product_category_name_english,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    latest_correction_effective_at,
    latest_change_reason,
    valid_from,
    next_valid_from as valid_to,
    coalesce(next_valid_from is null, false) as is_current,
    dbt_valid_from as snapshot_valid_from,
    dbt_valid_to as snapshot_valid_to
from scd2_windows
