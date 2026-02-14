{{ config(materialized='table') }}

with fct as (
  select * from {{ ref('fct_orders') }}
),
dim_products as (
  select product_id from {{ ref('dim_products') }}
),
dim_suppliers as (
  select supplier_id from {{ ref('dim_suppliers') }}
),
dim_warehouses as (
  select warehouse_id from {{ ref('dim_warehouses') }}
)

select
  current_timestamp() as dq_run_ts,

  -- volumes
  (select count(*) from fct) as fct_orders_row_count,

  -- null checks
  (select count(*) from fct where order_id is null) as null_order_id,
  (select count(*) from fct where product_id is null) as null_product_id,
  (select count(*) from fct where supplier_id is null) as null_supplier_id,
  (select count(*) from fct where warehouse_id is null) as null_warehouse_id,

  -- relationship “orphans”
  (select count(*) from fct f left join dim_products d using(product_id) where d.product_id is null) as orphan_product_id,
  (select count(*) from fct f left join dim_suppliers d using(supplier_id) where d.supplier_id is null) as orphan_supplier_id,
  (select count(*) from fct f left join dim_warehouses d using(warehouse_id) where d.warehouse_id is null) as orphan_warehouse_id,

  -- freshness-ish signal
  (select max(order_date) from fct) as max_order_date
