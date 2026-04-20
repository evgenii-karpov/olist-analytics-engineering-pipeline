{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_unique_id',
        strategy='check',
        check_cols=[
            'customer_zip_code_prefix',
            'customer_city',
            'customer_state'
        ],
        invalidate_hard_deletes=True
    )
}}

select
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    latest_correction_effective_at,
    latest_change_reason,
    _loaded_at
from {{ ref('int_customer_current_attributes') }}

{% endsnapshot %}
