with r as (
  select review_score
  from {{ ref('fact_order_reviews') }}
  where review_score is not null
)

select
  count(*) as total_reviews,
  sum(case when review_score in (4,5) then 1 else 0 end) as promoters,
  sum(case when review_score = 3 then 1 else 0 end) as passives,
  sum(case when review_score in (1,2) then 1 else 0 end) as detractors,
  (sum(case when review_score in (4,5) then 1 else 0 end)::float / nullif(count(*),0)) as pct_promoters,
  (sum(case when review_score in (1,2) then 1 else 0 end)::float / nullif(count(*),0)) as pct_detractors,
  (
    (sum(case when review_score in (4,5) then 1 else 0 end)::float / nullif(count(*),0))
    -
    (sum(case when review_score in (1,2) then 1 else 0 end)::float / nullif(count(*),0))
  ) as nps_proxy_score
from r