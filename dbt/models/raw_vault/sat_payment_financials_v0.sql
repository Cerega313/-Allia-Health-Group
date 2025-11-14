{{ config(materialized='incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'hk_payment_h'         
src_hashdiff: 'hd_payment_financials_s'
src_payload:
  - copay_amount
  - insurance_amount
  - total_amount
  - cost_amount
  - profit_amount
source_model: 'stg_dv_lifefile__payments'
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}

