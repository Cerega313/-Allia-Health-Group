{{ config(materialized = 'view') }}

with v0 as (
    select
      hk_payment_h,
      ldts as valid_from,
      copay_amount,
      insurance_amount,
      total_amount,
      cost_amount,
      profit_amount
    from {{ ref('sat_payment_financials_v0') }}
),

scd2 as (
    select
      hk_payment_h,
      valid_from,
      lead(valid_from) over (partition by hk_payment_h order by valid_from) as valid_to,
      copay_amount,
      insurance_amount,
      total_amount,
      cost_amount,
      profit_amount
    from v0
)

select
  hk_payment_h,
  valid_from,
  coalesce(valid_to, timestamp '9999-12-31 00:00:00') as valid_to,
  case when valid_to is null then true else false end as is_current,
  copay_amount,
  insurance_amount,
  total_amount,
  cost_amount,
  profit_amount
from scd2;
