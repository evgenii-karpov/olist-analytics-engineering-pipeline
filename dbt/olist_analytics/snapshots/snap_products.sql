{% snapshot snap_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=[
            'product_category_name',
            'product_category_name_english',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm'
        ],
        invalidate_hard_deletes=True
    )
}}

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
    _loaded_at
from {{ ref('int_product_current_attributes') }}

{% endsnapshot %}
