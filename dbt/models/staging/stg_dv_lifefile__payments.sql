{{ config(materialized = 'view') }}

{%- set yaml_metadata -%}
source_model:
  lifefile: stg_lifefile__payments 

ldts: updated_at
rsrc: "!lifefile/payments"

business_keys:
  - payment_id
  - order_id
  - payer_id
  - provider_id

hashed_columns:
  hk_payment_h:
    - payment_id

  hk_order_h:
    - order_id

  hk_payer_h:
    - payer_id

  hk_provider_h:
    - provider_id

  # LINKS
  hk_payment_order_l:
    - payment_id
    - order_id

  hk_payment_payer_l:
    - payment_id
    - payer_id

  hk_payment_payer_l:
    - payment_id
    - provider_id

  # Satellites
  hd_payment_financials_s:
    - copay_amount
    - insurance_amount
    - total_amount
    - cost_amount
    - profit_amount

  hd_payment_context_s:
    - payment_date
    - posting_date
    - status
    - payment_method
    - payer_type
    - is_refund
    
  hd_payment_technical_s:
    - created_at
    - gcs_uri

{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata = yaml_metadata) }}

