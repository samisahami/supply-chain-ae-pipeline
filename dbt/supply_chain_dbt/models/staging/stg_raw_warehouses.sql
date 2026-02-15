{{ config(materialized='view') }}

select
  concat('W', lpad(regexp_extract(warehouse_id, r'\d+'), 2, '0')) as warehouse_id,
  cast(city as string) as city,
  cast(state as string) as state
from {{ ref('raw_warehouses') }}
