update stg.orders
set
  year  = extract(year  from order_purchase_timestamp),
  month = extract(month from order_purchase_timestamp)
where year is null or month is null;
