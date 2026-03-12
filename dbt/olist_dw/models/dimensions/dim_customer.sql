{{
  config(
    materialized='table'
  )
}}

with c as (
    select *
    from {{ ref('stg_customers') }}
),

dedup as (
    select distinct *
    from c
)

select
  row_number() over(order by customer_id) as customer_sk,
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  ingestion_date
from dedup