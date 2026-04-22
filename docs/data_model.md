# Data Model

## Modeling Goals

The core model should demonstrate dimensional modeling, star schema design,
grain awareness, SCD Type 2 dimensions, incremental facts, and business-facing
marts.

## Source Entities

Expected Olist files:

- customers
- geolocation
- order_items
- order_payments
- order_reviews
- orders
- products
- sellers
- product_category_name_translation

## Warehouse Layers

```text
raw
  raw source tables loaded from the local raw zone or S3-compatible path

staging
  typed, cleaned views over raw tables

intermediate
  reusable transformation logic

snapshots
  dbt-managed SCD2 records

core
  dimensional model

marts
  business aggregates
```

## Grain Decisions

### fact_order_items

Grain:

```text
one row per order_id + order_item_id
```

Why this grain:

- Olist order items naturally identify products and sellers.
- Product revenue and freight values are available at item level.
- Payments are at order level, so payment allocation can demonstrate careful
  multi-grain modeling.

Candidate measures:

- `price`
- `freight_value`
- `gross_item_amount`
- `allocated_payment_value`
- `delivery_days`
- `delivery_delay_days`
- `is_delivered_late`

Candidate degenerate dimensions:

- `order_id`
- `order_item_id`

## Core Dimensions

### dim_customer_scd2

Business key:

```text
customer_unique_id
```

SCD2 attributes:

- `customer_zip_code_prefix`
- `customer_city`
- `customer_state`

Use case:

Customer profile and address changes over time.

### dim_product_scd2

Business key:

```text
product_id
```

SCD2 attributes:

- `product_category_name`
- `product_category_name_english`
- `product_weight_g`
- `product_length_cm`
- `product_height_cm`
- `product_width_cm`

Use case:

Product attribute corrections and category reclassification over time.

### dim_seller

Business key:

```text
seller_id
```

Initial type:

```text
Type 1 table
```

Reason:

It gives the project a useful contrast between Type 1 and Type 2 dimensions.
Seller SCD2 can be added later if needed.

### dim_date

Business key:

```text
date_day
```

Used for:

- Purchase date.
- Approval date.
- Delivered date.
- Estimated delivery date.

### dim_order_status

Small reference dimension based on order status values.

Expected statuses include:

- `created`
- `approved`
- `invoiced`
- `processing`
- `shipped`
- `delivered`
- `unavailable`
- `canceled`

## SCD2 Strategy

Olist is a static dataset, so real source updates are not naturally visible
across ingestion runs. To demonstrate SCD2 in a production-like way, the project
will add controlled correction feeds.

Correction feeds:

```text
customer_profile_changes
product_attribute_changes
```

These feeds simulate master-data updates such as:

- Customer address/profile updates.
- Product category corrections.
- Product size or weight corrections.

The important principle is that source entities can appear more than once across
batches with the same business key and changed tracked attributes.

## dbt Snapshots

Snapshots will use the `check` strategy.

Customer snapshot:

```text
unique_key: customer_unique_id
check_cols:
  - customer_zip_code_prefix
  - customer_city
  - customer_state
```

Product snapshot:

```text
unique_key: product_id
check_cols:
  - product_category_name
  - product_category_name_english
  - product_weight_g
  - product_length_cm
  - product_height_cm
  - product_width_cm
```

Snapshot outputs are transformed into core dimensions with clean column names:

```text
valid_from
valid_to
is_current
```

The core dimensions also add a baseline row from the original source attributes
with `valid_from = 1900-01-01`. This keeps historical facts joinable even when a
demo run is started at a final `batch_date` where correction feeds are already
visible.

For fact joins, `valid_from` and `valid_to` represent business-effective
windows. dbt snapshot metadata columns are retained separately as
`snapshot_valid_from` and `snapshot_valid_to`, because dbt snapshot timestamps
represent processing time rather than the business event time used by facts.

## Fact To SCD2 Joins

The fact table should join to SCD2 dimensions using the event timestamp.

Example:

```sql
orders.order_purchase_timestamp >= customer.valid_from
and orders.order_purchase_timestamp < coalesce(customer.valid_to, '9999-12-31')
```

This allows historical facts to be analyzed with the dimension attributes that
were valid when the business event happened.

## Payment Allocation

Olist payments are at order grain, while `fact_order_items` is at order item
grain.

The project will allocate order-level payment value to order items
proportionally:

```text
item_gross_amount = price + freight_value
allocated_payment_value =
  order_payment_value * item_gross_amount / order_gross_amount
```

This demonstrates careful handling of different source grains.

## Incremental Reprocessing Window

`fact_order_items` is incremental, but the reprocessing window is intentionally
slightly wider than a simple "last N days" filter. Each run reprocesses from the
earliest of:

- the configured late-arriving lookback boundary;
- the earliest visible customer correction effective timestamp;
- the earliest visible product correction effective timestamp.

This keeps fact-to-SCD2 surrogate keys consistent when a dimension correction is
business-effective in the past.

## Materializations

Recommended first version:

```text
staging: views
intermediate: views or ephemeral models
snapshots: dbt snapshot tables
dimensions: tables
fact_order_items: incremental table
marts: tables
```

Rationale:

- Staging views stay lightweight and transparent.
- Dimensions are small and cheap to rebuild.
- The main fact is the best place to demonstrate incremental processing.
- Marts are small aggregates, so table rebuilds are simple and reliable.

## Marts

### mart_daily_revenue

Grain:

```text
one row per order_purchase_date
```

Metrics:

- `gross_revenue`
- `product_revenue`
- `freight_revenue`
- `orders_count`
- `customers_count`
- `items_count`
- `average_order_value`
- `average_delivery_days`
- `late_deliveries_count`

### mart_monthly_arpu

Grain:

```text
one row per month
```

Metrics:

- `active_customers`
- `total_revenue`
- `arpu`
- `orders_per_customer`
- `average_order_value`
- `repeat_customer_rate`

## Testing Strategy

### Source And Staging Tests

- Primary keys are not null and unique where applicable.
- Foreign keys have relationships to parent sources.
- Status fields use accepted values.
- Monetary fields are non-negative.
- Timestamps parse successfully.

### Core Tests

- Dimension surrogate keys are unique and not null.
- Fact natural key is unique and not null.
- Fact foreign keys have relationships to dimensions.
- Fact rows match the cleaned staging order-item grain.
- SCD2 windows do not overlap per business key.
- SCD2 windows have positive effective intervals.
- Current SCD2 row count is at most one per business key.
- Payment allocations balance back to order-level payments.

### Mart Tests

- Mart grain is unique.
- Revenue and customer counts are non-negative.
- ARPU equals total revenue divided by active customers.
- Dates are within expected source date bounds.
