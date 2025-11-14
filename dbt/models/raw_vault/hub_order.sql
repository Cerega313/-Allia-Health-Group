{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'hk_order_h'
business_keys:
  - order_id
source_models: 'stg_dv_lifefile__payments'
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
