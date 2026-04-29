-- Raw correction feed tables for SCD2 simulation in local PostgreSQL.

create table if not exists raw_data.customer_profile_changes (
    customer_unique_id varchar(256),
    effective_at timestamp,
    customer_zip_code_prefix varchar(16),
    customer_city varchar(256),
    customer_state varchar(2),
    change_reason varchar(256),
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);

create table if not exists raw_data.product_attribute_changes (
    product_id varchar(256),
    effective_at timestamp,
    product_category_name varchar(256),
    product_weight_g integer,
    product_length_cm integer,
    product_height_cm integer,
    product_width_cm integer,
    change_reason varchar(256),
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);
