with dates as (
    select distinct cast(order_purchase_timestamp as date) as date_day
    from {{ ref('stg_orders') }}
    where order_purchase_timestamp is not null

    union

    select distinct cast(review_creation_ts as date) as date_day
    from {{ ref('stg_order_reviews') }}
    where review_creation_ts is not null
)
select
  date_day,
  extract(year  from date_day) as year,
  extract(month from date_day) as month,
  extract(day   from date_day) as day,
  extract(dow   from date_day) as day_of_week
from dates