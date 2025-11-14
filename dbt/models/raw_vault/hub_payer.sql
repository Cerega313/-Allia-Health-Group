{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'hk_payer_h'
business_keys:
  - payer_id
source_models: 'stg_dv_lifefile__payments'
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
