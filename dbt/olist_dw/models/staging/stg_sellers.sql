with source as (
    select *
    from {{ source('olist_stg', 'sellers') }}
)
select
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state,
  ingestion_date
from source