{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
  )
}}

with src as (

    select
        order_id,
        order_date,
        product_id,
        warehouse_id,
        supplier_id,
        quantity,
        order_revenue,

        -- Derived metrics
        safe_divide(order_revenue, quantity) as avg_unit_price

    from {{ ref('stg_raw_orders') }}

    {% if is_incremental() %}
      -- Only pull “new-ish” data on incremental runs (BigQuery)
      where order_id > (select max(order_id) from {{ this }})
    {% endif %}

)

select * from src
