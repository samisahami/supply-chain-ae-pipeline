{% snapshot suppliers_snapshot %}

{{
    config(
      target_schema='analytics_supply_chain',
      unique_key='supplier_id',
      strategy='check',
      check_cols=['supplier_name', 'country_code', 'base_lead_time_days', 'reliability_score']
    )
}}

select
    supplier_id,
    supplier_name,
    country_code,
    base_lead_time_days,
    reliability_score
from {{ ref('dim_suppliers') }}

{% endsnapshot %}
