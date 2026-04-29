-- Redshift COPY template for raw Olist tables.
-- Replace placeholders before running:
--   <bucket>
--   <prefix>
--   <batch_date>
--   <run_id>
--   <redshift_iam_role_arn>

-- Example S3 path:
-- s3://<bucket>/<prefix>/raw/orders/batch_date=<batch_date>/run_id=<run_id>/orders.csv.gz

copy raw_data.customers
from 's3://<bucket>/<prefix>/raw/customers/batch_date=<batch_date>/run_id=<run_id>/'
iam_role '<redshift_iam_role_arn>'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
emptyasnull
blanksasnull
acceptinvchars
region '<aws_region>';

copy raw_data.geolocation
from 's3://<bucket>/<prefix>/raw/geolocation/batch_date=<batch_date>/run_id=<run_id>/'
iam_role '<redshift_iam_role_arn>'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
emptyasnull
blanksasnull
acceptinvchars
region '<aws_region>';

copy raw_data.order_items
from 's3://<bucket>/<prefix>/raw/order_items/batch_date=<batch_date>/run_id=<run_id>/'
iam_role '<redshift_iam_role_arn>'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
emptyasnull
blanksasnull
acceptinvchars
region '<aws_region>';

copy raw_data.order_payments
from 's3://<bucket>/<prefix>/raw/order_payments/batch_date=<batch_date>/run_id=<run_id>/'
iam_role '<redshift_iam_role_arn>'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
emptyasnull
blanksasnull
acceptinvchars
region '<aws_region>';

copy raw_data.order_reviews
from 's3://<bucket>/<prefix>/raw/order_reviews/batch_date=<batch_date>/run_id=<run_id>/'
iam_role '<redshift_iam_role_arn>'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
emptyasnull
blanksasnull
acceptinvchars
region '<aws_region>';

copy raw_data.orders
from 's3://<bucket>/<prefix>/raw/orders/batch_date=<batch_date>/run_id=<run_id>/'
iam_role '<redshift_iam_role_arn>'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
emptyasnull
blanksasnull
acceptinvchars
region '<aws_region>';

copy raw_data.products
from 's3://<bucket>/<prefix>/raw/products/batch_date=<batch_date>/run_id=<run_id>/'
iam_role '<redshift_iam_role_arn>'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
emptyasnull
blanksasnull
acceptinvchars
region '<aws_region>';

copy raw_data.sellers
from 's3://<bucket>/<prefix>/raw/sellers/batch_date=<batch_date>/run_id=<run_id>/'
iam_role '<redshift_iam_role_arn>'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
emptyasnull
blanksasnull
acceptinvchars
region '<aws_region>';

copy raw_data.product_category_translation
from 's3://<bucket>/<prefix>/raw/product_category_translation/batch_date=<batch_date>/run_id=<run_id>/'
iam_role '<redshift_iam_role_arn>'
csv
gzip
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
emptyasnull
blanksasnull
acceptinvchars
region '<aws_region>';
