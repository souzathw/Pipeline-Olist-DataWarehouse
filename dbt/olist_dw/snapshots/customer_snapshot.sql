{% snapshot customer_snapshot %}

{{
    config(
      target_schema='dw_dw',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_city', 'customer_state']
    )
}}

select *
from {{ source('olist_stg', 'customers') }}

{% endsnapshot %}