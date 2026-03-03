with s as (
    select *
    from {{ ref('stg_sellers') }}
)
select
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state,
  ingestion_date
from s