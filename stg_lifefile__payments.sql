{{ config(materialized = 'view') }}

with raw as (

    select *
    from {{ source('lifefile', 'payments_raw') }}

),

clean as (
  select
        -- keys
        payment_id,
        order_id,

        -- dates
        SAFE_CAST(created_at   AS timestamp) as created_at,
        SAFE_CAST(updated_at   AS timestamp) as updated_at,
        SAFE_CAST(payment_date AS timestamp) as payment_date,
        SAFE_CAST(posting_date AS timestamp) as posting_date,

        -- numbers 
        SAFE_CAST(copay_amount      AS numeric) as copay_amount,
        SAFE_CAST(insurance_amount  AS numeric) as insurance_amount,
        SAFE_CAST(total_amount      AS numeric) as total_amount,
        SAFE_CAST(cost_amount       AS numeric) as cost_amount,
        SAFE_CAST(profit_amount     AS numeric) as profit_amount,

        -- payer data
        payer_type,
        payer_id,
        payment_method,
        status,
  
        (case when is_refund is null then false
              when lower(cast(is_refund as string)) in ('true','t','1','yes','y')  then true
              when lower(cast(is_refund as string)) in ('false','f','0','no','n') then false
              else SAFE_CAST(is_refund as bool)
              end) as is_refund,

        -- technical field
        gcs_uri

    from raw
)

select *
from clean;
