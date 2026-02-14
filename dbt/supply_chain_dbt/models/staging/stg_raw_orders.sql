{{ config(materialized='view') }}

select
    cast(order_id as string) as order_id,
    cast(order_date as date) as order_date,
    cast(product_id as string) as product_id,
    cast(warehouse_id as string) as warehouse_id,
    cast(supplier_id as string) as supplier_id,
    cast(order_qty as int64) as quantity,
    cast(order_value_usd as float64) as order_revenue
from {{ ref('raw_orders') }}

