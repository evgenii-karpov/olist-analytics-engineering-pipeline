-- Raw correction feed tables for SCD2 simulation.
-- These tables are loaded from generated correction feeds.

create table if not exists raw_data.customer_profile_changes (
    customer_unique_id varchar(256) encode zstd,
    effective_at timestamp encode zstd,
    customer_zip_code_prefix varchar(16) encode zstd,
    customer_city varchar(256) encode zstd,
    customer_state varchar(2) encode zstd,
    change_reason varchar(256) encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);

create table if not exists raw_data.product_attribute_changes (
    product_id varchar(256) encode zstd,
    effective_at timestamp encode zstd,
    product_category_name varchar(256) encode zstd,
    product_weight_g integer encode zstd,
    product_length_cm integer encode zstd,
    product_height_cm integer encode zstd,
    product_width_cm integer encode zstd,
    change_reason varchar(256) encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);
