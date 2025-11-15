{{ config(materialized = 'incremental') }}

{%- set yaml_metadata -%}
link_hashkey: 'hk_payment_provider_l' 
foreign_hashkeys:
  - 'hk_payment_h'           
  - 'hk_provider_h'         
source_models:
  - name: stg_dv_lifefile__payments
{%- endset -%}

{{ datavault4dbt.link(yaml_metadata = yaml_metadata) }}
