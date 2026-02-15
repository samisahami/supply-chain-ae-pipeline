{{ config(materialized='view') }}

select
  concat('P', lpad(regexp_extract(product_id, r'\d+'), 5, '0')) as product_id,
  cast(category as string) as product_category,
  cast(primary_supplier_id as string) as primary_supplier_id,
  cast(unit_cost_usd as float64) as unit_cost_usd
from {{ ref('raw_products') }}
