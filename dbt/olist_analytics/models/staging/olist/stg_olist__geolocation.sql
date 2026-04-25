with ranked as (
    select
        geolocation_zip_code_prefix::varchar(16) as geolocation_zip_code_prefix,
        geolocation_lat::decimal(18, 14) as geolocation_lat,
        geolocation_lng::decimal(18, 14) as geolocation_lng,
        lower(trim(geolocation_city))::varchar(256) as geolocation_city,
        upper(trim(geolocation_state))::varchar(2) as geolocation_state,
        _batch_id,
        _loaded_at,
        _source_file,
        _source_system,
        row_number() over (
            partition by
                geolocation_zip_code_prefix,
                geolocation_lat,
                geolocation_lng,
                lower(trim(geolocation_city)),
                upper(trim(geolocation_state))
            order by _loaded_at desc, _batch_id desc
        ) as row_number
    from {{ source('olist', 'geolocation') }}
)

select
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    _batch_id,
    _loaded_at,
    _source_file,
    _source_system
from ranked
where row_number = 1
