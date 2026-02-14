{{ config(materialized='table') }}

select
  product_id,
  product_category,
  primary_supplier_id,
  unit_cost_usd
from {{ ref('stg_raw_products') }}


