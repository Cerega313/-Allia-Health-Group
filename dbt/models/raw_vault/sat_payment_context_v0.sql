{{ config(materialized = 'incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'hk_payment_h'
src_hashdiff: 'hd_payment_context_s'
src_payload:
  - payment_date
  - posting_date
  - status
  - payment_method
  - payer_type
  - is_refund
source_model: 'stg_dv_lifefile__payments'
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata = yaml_metadata) }}
