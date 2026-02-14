{{ config(materialized='view') }}

select
  cast(product_id as string) as product_id,
  cast(category as string) as product_category,
  cast(primary_supplier_id as string) as primary_supplier_id,
  cast(unit_cost_usd as float64) as unit_cost_usd
from {{ source('raw', 'raw_products') }}

