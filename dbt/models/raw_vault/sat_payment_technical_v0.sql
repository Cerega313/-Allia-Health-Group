{{ config(materialized = 'incremental') }}

{%- set yaml_metadata -%}
parent_hashkey: 'hk_payment_h'
src_hashdiff: 'hd_payment_technical_s'
src_payload:
  - created_at
  - gcs_uri
source_model: 'stg_dv_lifefile__payments'
{%- endset -%}

{{ datavault4dbt.sat_v0(yaml_metadata = yaml_metadata) }}
