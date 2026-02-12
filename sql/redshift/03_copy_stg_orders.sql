truncate table stg.orders;

copy stg.orders (
  order_id,
  customer_id,
  order_status,
  order_purchase_timestamp,
  order_approved_at,
  order_delivered_carrier_date,
  order_delivered_customer_date,
  order_estimated_delivery_date,
  ingestion_date
)
from 's3://de-olist-thiago-dev/bronze/olist/orders/ingestion_date={{INGESTION_DATE}}/'
iam_role '{{REDSHIFT_IAM_ROLE_ARN}}'
format as parquet;
