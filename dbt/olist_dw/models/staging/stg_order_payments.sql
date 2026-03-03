with source as (
    select *
    from {{ source('olist_stg', 'order_payments') }}
)
select
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value,
  ingestion_date
from source