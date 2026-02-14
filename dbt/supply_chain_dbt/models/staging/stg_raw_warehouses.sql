{{ config(materialized='view') }}

select
  cast(warehouse_id as string) as warehouse_id,
  cast(city as string) as city,
  cast(state as string) as state
from {{ source('raw', 'raw_warehouses') }}
