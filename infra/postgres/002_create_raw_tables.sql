-- Raw Olist source tables for local PostgreSQL.
-- Raw tables are append-only and include ingestion metadata columns.

create table if not exists raw.customers (
    customer_id varchar(256),
    customer_unique_id varchar(256),
    customer_zip_code_prefix varchar(16),
    customer_city varchar(256),
    customer_state varchar(256),
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);

create table if not exists raw.geolocation (
    geolocation_zip_code_prefix varchar(16),
    geolocation_lat decimal(18, 14),
    geolocation_lng decimal(18, 14),
    geolocation_city varchar(256),
    geolocation_state varchar(256),
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);

create table if not exists raw.order_items (
    order_id varchar(256),
    order_item_id integer,
    product_id varchar(256),
    seller_id varchar(256),
    shipping_limit_date timestamp,
    price decimal(18, 2),
    freight_value decimal(18, 2),
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);

create table if not exists raw.order_payments (
    order_id varchar(256),
    payment_sequential integer,
    payment_type varchar(256),
    payment_installments integer,
    payment_value decimal(18, 2),
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);

create table if not exists raw.order_reviews (
    review_id varchar(256),
    order_id varchar(256),
    review_score integer,
    review_comment_title varchar(1024),
    review_comment_message varchar(65535),
    review_creation_date timestamp,
    review_answer_timestamp timestamp,
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);

create table if not exists raw.orders (
    order_id varchar(256),
    customer_id varchar(256),
    order_status varchar(256),
    order_purchase_timestamp timestamp,
    order_approved_at timestamp,
    order_delivered_carrier_date timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp,
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);

create table if not exists raw.products (
    product_id varchar(256),
    product_category_name varchar(256),
    product_name_lenght integer,
    product_description_lenght integer,
    product_photos_qty integer,
    product_weight_g integer,
    product_length_cm integer,
    product_height_cm integer,
    product_width_cm integer,
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);

create table if not exists raw.sellers (
    seller_id varchar(256),
    seller_zip_code_prefix varchar(16),
    seller_city varchar(256),
    seller_state varchar(256),
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);

create table if not exists raw.product_category_translation (
    product_category_name varchar(256),
    product_category_name_english varchar(256),
    _batch_id varchar(128) not null,
    _loaded_at timestamp not null,
    _source_file varchar(512) not null,
    _source_system varchar(64) not null
);
