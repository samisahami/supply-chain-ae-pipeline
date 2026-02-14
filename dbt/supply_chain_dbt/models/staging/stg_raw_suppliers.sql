{{ config(materialized='view') }}

select
  cast(supplier_id as string) as supplier_id,
  cast(supplier_name as string) as supplier_name,
  cast(country_code as string) as country_code,
  cast(base_lead_time_days as int64) as base_lead_time_days,
  cast(reliability_score as float64) as reliability_score
from {{ source('raw', 'raw_suppliers') }}

