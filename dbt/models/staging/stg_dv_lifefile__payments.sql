{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model:
  lifefile: stg_lifefile__payments 

ldts: updated_at
rsrc: "!lifefile/payments"

hashed_columns:
  hk_payment_h:
    - payment_id

  hd_payment_financials_s:
    - copay_amount
    - insurance_amount
    - total_amount
    - cost_amount
    - profit_amount
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
