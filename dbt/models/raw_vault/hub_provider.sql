{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
hashkey: 'hk_provider_h'
business_keys:
  - provider_id
source_models: 'stg_lifefile__providers'
{%- endset -%}

{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
