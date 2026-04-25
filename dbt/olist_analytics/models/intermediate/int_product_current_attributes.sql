with products as (
    select
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        _loaded_at
    from {{ ref('stg_olist__products') }}
),

product_category_translations as (
    select
        product_category_name,
        product_category_name_english
    from {{ ref('stg_olist__product_category_translation') }}
),

corrections as (
    select
        product_id,
        effective_at,
        product_category_name,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        change_reason,
        _loaded_at
    from {{ ref('stg_olist__product_attribute_changes') }}
),

products_with_translations as (
    select
        products.product_id,
        products.product_category_name,
        product_category_translations.product_category_name_english,
        products.product_name_length,
        products.product_description_length,
        products.product_photos_qty,
        products.product_weight_g,
        products.product_length_cm,
        products.product_height_cm,
        products.product_width_cm,
        products._loaded_at
    from products
    left join product_category_translations
        on
            products.product_category_name
            = product_category_translations.product_category_name
),

corrections_ranked as (
    select
        corrections.product_id,
        corrections.effective_at,
        corrections.product_category_name,
        translations.product_category_name_english,
        corrections.product_weight_g,
        corrections.product_length_cm,
        corrections.product_height_cm,
        corrections.product_width_cm,
        corrections.change_reason,
        corrections._loaded_at,
        row_number() over (
            partition by corrections.product_id
            order by corrections.effective_at desc, corrections._loaded_at desc
        ) as row_number
    from corrections
    left join product_category_translations as translations
        on
            corrections.product_category_name
            = translations.product_category_name
    where
        corrections.effective_at
        <= '{{ var("batch_date", "9999-12-31") }}'::timestamp
),

latest_corrections as (
    select
        product_id,
        effective_at,
        product_category_name,
        product_category_name_english,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        change_reason,
        _loaded_at
    from corrections_ranked
    where row_number = 1
)

select
    products.product_id,
    coalesce(
        latest_corrections.product_category_name,
        products.product_category_name
    ) as product_category_name,
    coalesce(
        latest_corrections.product_category_name_english,
        products.product_category_name_english
    ) as product_category_name_english,
    products.product_name_length,
    products.product_description_length,
    products.product_photos_qty,
    coalesce(
        latest_corrections.product_weight_g,
        products.product_weight_g
    ) as product_weight_g,
    coalesce(
        latest_corrections.product_length_cm,
        products.product_length_cm
    ) as product_length_cm,
    coalesce(
        latest_corrections.product_height_cm,
        products.product_height_cm
    ) as product_height_cm,
    coalesce(
        latest_corrections.product_width_cm,
        products.product_width_cm
    ) as product_width_cm,
    latest_corrections.effective_at as latest_correction_effective_at,
    latest_corrections.change_reason as latest_change_reason,
    greatest(
        products._loaded_at,
        coalesce(latest_corrections._loaded_at, products._loaded_at)
    ) as _loaded_at
from products_with_translations as products
left join latest_corrections
    on products.product_id = latest_corrections.product_id
