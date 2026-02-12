select count(*) as rows_loaded from stg.orders;

select
  sum(case when year is null then 1 else 0 end) as year_nulls,
  sum(case when month is null then 1 else 0 end) as month_nulls
from stg.orders;

select
  min(order_purchase_timestamp) as min_purchase_ts,
  max(order_purchase_timestamp) as max_purchase_ts
from stg.orders;
