{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['order_id', 'payment_sequential']
  )
}}

with p as (
    select *
    from {{ ref('stg_order_payments') }}

    {% if is_incremental() %}
      where ingestion_date = '{{ var("ingestion_date") }}'
    {% endif %}
)

select
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value,
  ingestion_date
from p
where order_id is not null