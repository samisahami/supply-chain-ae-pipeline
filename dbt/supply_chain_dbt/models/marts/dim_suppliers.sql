{{ config(materialized='table') }}

select
  supplier_id,
  supplier_name,
  country_code,
  base_lead_time_days,
  reliability_score
from {{ ref('stg_raw_suppliers') }}


