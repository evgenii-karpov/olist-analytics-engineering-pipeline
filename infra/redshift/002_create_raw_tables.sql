-- Raw Olist source tables.
-- Generated from docs/source_profile.json.
-- Raw tables are append-only and include ingestion metadata columns.

create table if not exists raw_data.customers (
    customer_id varchar(256) encode zstd,
    customer_unique_id varchar(256) encode zstd,
    customer_zip_code_prefix varchar(16) encode zstd,
    customer_city varchar(256) encode zstd,
    customer_state varchar(256) encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);

create table if not exists raw_data.geolocation (
    geolocation_zip_code_prefix varchar(16) encode zstd,
    geolocation_lat decimal(18, 14) encode zstd,
    geolocation_lng decimal(18, 14) encode zstd,
    geolocation_city varchar(256) encode zstd,
    geolocation_state varchar(256) encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);

create table if not exists raw_data.order_items (
    order_id varchar(256) encode zstd,
    order_item_id integer encode zstd,
    product_id varchar(256) encode zstd,
    seller_id varchar(256) encode zstd,
    shipping_limit_date timestamp encode zstd,
    price decimal(18, 2) encode zstd,
    freight_value decimal(18, 2) encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);

create table if not exists raw_data.order_payments (
    order_id varchar(256) encode zstd,
    payment_sequential integer encode zstd,
    payment_type varchar(256) encode zstd,
    payment_installments integer encode zstd,
    payment_value decimal(18, 2) encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);

create table if not exists raw_data.order_reviews (
    review_id varchar(256) encode zstd,
    order_id varchar(256) encode zstd,
    review_score integer encode zstd,
    review_comment_title varchar(1024) encode zstd,
    review_comment_message varchar(65535) encode zstd,
    review_creation_date timestamp encode zstd,
    review_answer_timestamp timestamp encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);

create table if not exists raw_data.orders (
    order_id varchar(256) encode zstd,
    customer_id varchar(256) encode zstd,
    order_status varchar(256) encode zstd,
    order_purchase_timestamp timestamp encode zstd,
    order_approved_at timestamp encode zstd,
    order_delivered_carrier_date timestamp encode zstd,
    order_delivered_customer_date timestamp encode zstd,
    order_estimated_delivery_date timestamp encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);

create table if not exists raw_data.products (
    product_id varchar(256) encode zstd,
    product_category_name varchar(256) encode zstd,
    product_name_lenght integer encode zstd,
    product_description_lenght integer encode zstd,
    product_photos_qty integer encode zstd,
    product_weight_g integer encode zstd,
    product_length_cm integer encode zstd,
    product_height_cm integer encode zstd,
    product_width_cm integer encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);

create table if not exists raw_data.sellers (
    seller_id varchar(256) encode zstd,
    seller_zip_code_prefix varchar(16) encode zstd,
    seller_city varchar(256) encode zstd,
    seller_state varchar(256) encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);

create table if not exists raw_data.product_category_translation (
    product_category_name varchar(256) encode zstd,
    product_category_name_english varchar(256) encode zstd,
    _batch_id varchar(128) not null encode zstd,
    _loaded_at timestamp not null encode zstd,
    _source_file varchar(512) not null encode zstd,
    _source_system varchar(64) not null encode zstd
)
diststyle auto
sortkey(_loaded_at);
