{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='review_id'
  )
}}

with r as (
    select *
    from {{ ref('stg_order_reviews') }}

    {% if is_incremental() %}
      where ingestion_date = '{{ var("ingestion_date") }}'
    {% endif %}
),

-- Dedup: garante 1 linha por review_id (pega o registro "mais recente")
dedup as (
    select
      r.*,
      row_number() over (
        partition by r.review_id
        order by r.ingestion_date desc, r.review_creation_ts desc
      ) as rn
    from r
),

valid as (
    select *
    from dedup
    where rn = 1
),


final as (
    select v.*
    from valid v
    where exists (
      select 1
      from {{ ref('fact_orders') }} o
      where o.order_id = v.order_id
    )
)

select
  review_id,
  order_id,
  review_score,
  review_comment_title,
  review_comment_message,
  review_creation_ts,
  review_answer_ts,
  cast(review_creation_ts as date) as review_creation_date,
  ingestion_date
from final