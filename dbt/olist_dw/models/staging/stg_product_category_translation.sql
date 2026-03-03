with source as (
    select *
    from {{ source('olist_stg', 'product_category_translation') }}
)
select
  product_category_name,
  product_category_name_english,
  ingestion_date
from source