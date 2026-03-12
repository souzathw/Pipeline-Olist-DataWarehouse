select
  cast(order_purchase_timestamp as date) as order_date,
  count(*) as orders
from {{ ref('fact_orders') }}
group by 1
order by 1