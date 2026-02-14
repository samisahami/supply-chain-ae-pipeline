{{ config(materialized='table') }}

select
  warehouse_id,
  city,
  state
from {{ ref('stg_raw_warehouses') }}

