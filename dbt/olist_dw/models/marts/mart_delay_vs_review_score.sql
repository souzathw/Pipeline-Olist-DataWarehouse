select
  o.delivery_delay_days as delay_days,
  avg(r.review_score::float) as avg_score,
  count(*) as n_reviews
from {{ ref('fact_orders') }} o
join {{ ref('fact_order_reviews') }} r
  on r.order_id = o.order_id
where o.delivery_delay_days is not null
group by 1
order by 1