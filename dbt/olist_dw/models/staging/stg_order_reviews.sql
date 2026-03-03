with source as (
    select *
    from {{ source('olist_stg', 'order_reviews') }}
),

clean as (
    select
      review_id,
      order_id,
      cast(review_score as smallint) as review_score,
      review_comment_title,
      review_comment_message,
      review_creation_ts,
      review_answer_ts,
      ingestion_date
    from source
    where review_id is not null
      and order_id is not null
      and review_score is not null
      and order_id is not null
      and order_id ~ '^[0-9a-f]{32}$'
)

select * from clean