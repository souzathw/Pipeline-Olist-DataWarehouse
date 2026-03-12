{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='order_id',
    dist='order_id',
    sort=['ingestion_date', 'order_purchase_timestamp']
  )
}}

with o as (
    select *
    from {{ ref('stg_orders') }}

    {% if is_incremental() %}
      where ingestion_date = '{{ var("ingestion_date") }}'
    {% endif %}
),

d as (
    select customer_sk, customer_id
    from {{ ref('dim_customer') }}
)

select
  o.order_id,
  d.customer_sk,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,

  case
    when o.order_delivered_customer_date is not null
     and o.order_estimated_delivery_date is not null
    then datediff(day, o.order_estimated_delivery_date, o.order_delivered_customer_date)
    else null
  end as delivery_delay_days,

  o.ingestion_date
from o
left join d
  on o.customer_id = d.customer_id
