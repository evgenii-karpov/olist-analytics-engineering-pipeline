# Source Contract

This document is generated from the local Olist dataset archive.

Source archive: `olist.zip`

## File Summary

| Entity | File | Rows | Columns |
| --- | --- | --- | --- |
| customers | `olist_customers_dataset.csv` | 99441 | 5 |
| geolocation | `olist_geolocation_dataset.csv` | 1000163 | 5 |
| order_items | `olist_order_items_dataset.csv` | 112650 | 7 |
| order_payments | `olist_order_payments_dataset.csv` | 103886 | 5 |
| order_reviews | `olist_order_reviews_dataset.csv` | 99224 | 7 |
| orders | `olist_orders_dataset.csv` | 99441 | 8 |
| products | `olist_products_dataset.csv` | 32951 | 9 |
| sellers | `olist_sellers_dataset.csv` | 3095 | 4 |
| product_category_translation | `product_category_name_translation.csv` | 71 | 2 |

## Expected Source Files

The ingestion layer should fail fast if any of these files are missing:

- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`

## Entity Contracts

The `Inferred type` column is based on sampled non-null values. The
`Redshift raw type` column is the recommended first-pass type for raw
DDL. Staging models can cast to stricter business types where needed.

### customers

Source file: `olist_customers_dataset.csv`

Rows: `99441`

| Column | Inferred type | Redshift raw type | Nulls | Null % | Sample values |
| --- | --- | --- | --- | --- | --- |
| `customer_id` | varchar | varchar(256) | 0 | 0.00% | `06b8999e2fba1a1fbc88172c00ba8bc7`, `18955e83d337fd6b2def6b18a428ac77`, `4e7b3e00288586ebd08712fdd0374a03` |
| `customer_unique_id` | varchar | varchar(256) | 0 | 0.00% | `861eff4711a542e4b93843c6dd7febb0`, `290c77bc529b7ac935b93aa66c333dc3`, `060e732b5b29e8181a18229c7b0b2b5e` |
| `customer_zip_code_prefix` | integer | varchar(16) | 0 | 0.00% | `14409`, `09790`, `01151` |
| `customer_city` | varchar | varchar(256) | 0 | 0.00% | `franca`, `sao bernardo do campo`, `sao paulo` |
| `customer_state` | varchar | varchar(256) | 0 | 0.00% | `SP`, `SC`, `MG` |

### geolocation

Source file: `olist_geolocation_dataset.csv`

Rows: `1000163`

| Column | Inferred type | Redshift raw type | Nulls | Null % | Sample values |
| --- | --- | --- | --- | --- | --- |
| `geolocation_zip_code_prefix` | integer | varchar(16) | 0 | 0.00% | `01037`, `01046`, `01041` |
| `geolocation_lat` | decimal | decimal(18, 14) | 0 | 0.00% | `-23.54562128115268`, `-23.546081127035535`, `-23.54612896641469` |
| `geolocation_lng` | decimal | decimal(18, 14) | 0 | 0.00% | `-46.63929204800168`, `-46.64482029837157`, `-46.64295148361138` |
| `geolocation_city` | varchar | varchar(256) | 0 | 0.00% | `sao paulo`, `são paulo`, `sao bernardo do campo` |
| `geolocation_state` | varchar | varchar(256) | 0 | 0.00% | `SP`, `RN`, `AC` |

### order_items

Source file: `olist_order_items_dataset.csv`

Rows: `112650`

| Column | Inferred type | Redshift raw type | Nulls | Null % | Sample values |
| --- | --- | --- | --- | --- | --- |
| `order_id` | varchar | varchar(256) | 0 | 0.00% | `00010242fe8c5a6d1ba2dd792cb16214`, `00018f77f2f0320c557190d7a144bdd3`, `000229ec398224ef6ca0657da4fc703e` |
| `order_item_id` | integer | integer | 0 | 0.00% | `1`, `2`, `3` |
| `product_id` | varchar | varchar(256) | 0 | 0.00% | `4244733e06e7ecb4970a6e2683c13e61`, `e5f2d52b802189ee658865ca93d83a8f`, `c777355d18b72b67abbeef9df44fd0fd` |
| `seller_id` | varchar | varchar(256) | 0 | 0.00% | `48436dade18ac8b2bce089ec2a041202`, `dd7ddc04e1b6c2c614352b383efe2d36`, `5b51032eddd242adc84c38acab88f23d` |
| `shipping_limit_date` | timestamp | timestamp | 0 | 0.00% | `2017-09-19 09:45:35`, `2017-05-03 11:05:13`, `2018-01-18 14:48:30` |
| `price` | decimal | decimal(18, 2) | 0 | 0.00% | `58.90`, `239.90`, `199.00` |
| `freight_value` | decimal | decimal(18, 2) | 0 | 0.00% | `13.29`, `19.93`, `17.87` |

### order_payments

Source file: `olist_order_payments_dataset.csv`

Rows: `103886`

| Column | Inferred type | Redshift raw type | Nulls | Null % | Sample values |
| --- | --- | --- | --- | --- | --- |
| `order_id` | varchar | varchar(256) | 0 | 0.00% | `b81ef226f3fe1789b1e8b2acac839d17`, `a9810da82917af2d9aefd1278f1dcfa0`, `25e8ea4e93396b6fa0d3dd708e76c1bd` |
| `payment_sequential` | integer | integer | 0 | 0.00% | `1`, `2`, `4` |
| `payment_type` | varchar | varchar(256) | 0 | 0.00% | `credit_card`, `boleto`, `voucher` |
| `payment_installments` | integer | integer | 0 | 0.00% | `8`, `1`, `2` |
| `payment_value` | decimal | decimal(18, 2) | 0 | 0.00% | `99.33`, `24.39`, `65.71` |

### order_reviews

Source file: `olist_order_reviews_dataset.csv`

Rows: `99224`

| Column | Inferred type | Redshift raw type | Nulls | Null % | Sample values |
| --- | --- | --- | --- | --- | --- |
| `review_id` | varchar | varchar(256) | 0 | 0.00% | `7bc2406110b926393aa56f80a40eba40`, `80e641a11e56f04c1ad469d5645fdfde`, `228ce5500dc1d8e020d8d1322874b6f0` |
| `order_id` | varchar | varchar(256) | 0 | 0.00% | `73fc7af87114b39712e6da79b0a377eb`, `a548910a1c6147796b98fdf73dbeba33`, `f9e4b658b201a9f2ecdecbb34bed034b` |
| `review_score` | integer | integer | 0 | 0.00% | `4`, `5`, `1` |
| `review_comment_title` | varchar | varchar(1024) | 87656 | 88.34% | `recomendo`, `Super recomendo`, `Não chegou meu produto` |
| `review_comment_message` | varchar | varchar(65535) | 58247 | 58.70% | `Recebi bem antes do prazo estipulado.`, `Parabéns lojas lannister adorei comprar pela Internet seguro e prático Parabéns a todos feliz Páscoa`, `aparelho eficiente. no site a marca do aparelho esta impresso como 3desinfector e ao chegar esta com outro nome...atualizar com a marca correta uma vez que é o mesmo aparelho` |
| `review_creation_date` | timestamp | timestamp | 0 | 0.00% | `2018-01-18 00:00:00`, `2018-03-10 00:00:00`, `2018-02-17 00:00:00` |
| `review_answer_timestamp` | timestamp | timestamp | 0 | 0.00% | `2018-01-18 21:46:59`, `2018-03-11 03:05:13`, `2018-02-18 14:36:24` |

### orders

Source file: `olist_orders_dataset.csv`

Rows: `99441`

| Column | Inferred type | Redshift raw type | Nulls | Null % | Sample values |
| --- | --- | --- | --- | --- | --- |
| `order_id` | varchar | varchar(256) | 0 | 0.00% | `e481f51cbdc54678b7cc49136f2d6af7`, `53cdb2fc8bc7dce0b6741e2150273451`, `47770eb9100c2d0c44946d9cf07ec65d` |
| `customer_id` | varchar | varchar(256) | 0 | 0.00% | `9ef432eb6251297304e76186b10a928d`, `b0830fb4747a6c6d20dea0b8c802d7ef`, `41ce2a54c0b03bf3443c3d931a367089` |
| `order_status` | varchar | varchar(256) | 0 | 0.00% | `delivered`, `invoiced`, `shipped` |
| `order_purchase_timestamp` | timestamp | timestamp | 0 | 0.00% | `2017-10-02 10:56:33`, `2018-07-24 20:41:37`, `2018-08-08 08:38:49` |
| `order_approved_at` | timestamp | timestamp | 160 | 0.16% | `2017-10-02 11:07:15`, `2018-07-26 03:24:27`, `2018-08-08 08:55:23` |
| `order_delivered_carrier_date` | timestamp | timestamp | 1783 | 1.79% | `2017-10-04 19:55:00`, `2018-07-26 14:31:00`, `2018-08-08 13:50:00` |
| `order_delivered_customer_date` | timestamp | timestamp | 2965 | 2.98% | `2017-10-10 21:25:13`, `2018-08-07 15:27:45`, `2018-08-17 18:06:29` |
| `order_estimated_delivery_date` | timestamp | timestamp | 0 | 0.00% | `2017-10-18 00:00:00`, `2018-08-13 00:00:00`, `2018-09-04 00:00:00` |

### products

Source file: `olist_products_dataset.csv`

Rows: `32951`

| Column | Inferred type | Redshift raw type | Nulls | Null % | Sample values |
| --- | --- | --- | --- | --- | --- |
| `product_id` | varchar | varchar(256) | 0 | 0.00% | `1e9e8ef04dbcff4541ed26657ea517e5`, `3aa071139cb16b67ca9e5dea641aaa2f`, `96bd76ec8810374ed1b65e291975717f` |
| `product_category_name` | varchar | varchar(256) | 610 | 1.85% | `perfumaria`, `artes`, `esporte_lazer` |
| `product_name_lenght` | integer | integer | 610 | 1.85% | `40`, `44`, `46` |
| `product_description_lenght` | integer | integer | 610 | 1.85% | `287`, `276`, `250` |
| `product_photos_qty` | integer | integer | 610 | 1.85% | `1`, `4`, `2` |
| `product_weight_g` | integer | integer | 2 | 0.01% | `225`, `1000`, `154` |
| `product_length_cm` | integer | integer | 2 | 0.01% | `16`, `30`, `18` |
| `product_height_cm` | integer | integer | 2 | 0.01% | `10`, `18`, `9` |
| `product_width_cm` | integer | integer | 2 | 0.01% | `14`, `20`, `15` |

### sellers

Source file: `olist_sellers_dataset.csv`

Rows: `3095`

| Column | Inferred type | Redshift raw type | Nulls | Null % | Sample values |
| --- | --- | --- | --- | --- | --- |
| `seller_id` | varchar | varchar(256) | 0 | 0.00% | `3442f8959a84dea7ee197c632cb2df15`, `d1b65fc7debc3361ea86b5f14c68d2e2`, `ce3ad9de960102d0677a81f5d0bb7b2d` |
| `seller_zip_code_prefix` | integer | varchar(16) | 0 | 0.00% | `13023`, `13844`, `20031` |
| `seller_city` | varchar | varchar(256) | 0 | 0.00% | `campinas`, `mogi guacu`, `rio de janeiro` |
| `seller_state` | varchar | varchar(256) | 0 | 0.00% | `SP`, `RJ`, `PE` |

### product_category_translation

Source file: `product_category_name_translation.csv`

Rows: `71`

| Column | Inferred type | Redshift raw type | Nulls | Null % | Sample values |
| --- | --- | --- | --- | --- | --- |
| `﻿product_category_name` | varchar | varchar(256) | 0 | 0.00% | `beleza_saude`, `informatica_acessorios`, `automotivo` |
| `product_category_name_english` | varchar | varchar(256) | 0 | 0.00% | `health_beauty`, `computers_accessories`, `auto` |

## Raw Metadata Columns

Every raw table should include these ingestion metadata columns:

| Column | Recommended type | Description |
| --- | --- | --- |
| `_batch_id` | varchar(128) | Deterministic batch identifier. |
| `_loaded_at` | timestamp | Warehouse load timestamp. |
| `_source_file` | varchar(512) | Original S3 object or source file. |
| `_source_system` | varchar(64) | Source system name, initially `olist_kaggle`. |

## Contract Rules

- Ingestion must fail if an expected file is missing.
- Ingestion must fail if a source header changes unexpectedly.
- Raw data should be append-only and batch-addressable.
- dbt staging models own stricter type casting and business naming.
- Correction feeds for SCD2 simulation should be versioned as separate source entities.
