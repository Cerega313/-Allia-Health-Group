{{ config(materialized = 'incremental', unique_key = 'payment_id') }}

with

-- Hubs
payment as (
  select hk_payment_h, payment_id
  from {{ ref('hub_payment') }}
),

"order" as (
  select hk_order_h, order_id
  from {{ ref('hub_order') }}
),

provider as (
  select hk_provider_h, provider_id
  from {{ ref('hub_provider') }}
),

-- Links
link_payment_order as (
  select hk_payment_h, hk_order_h
  from {{ ref('link_payment_order_l') }}
),

link_order_provider as (
  select hk_order_h, hk_provider_h
  from {{ ref('link_order_provider_l') }}
),

sat_fin_current as (
    select
      sf.*
    from {{ ref('sat_payment_financials_scd2') }} sf
  where is_current = true
),

sat_evt_current as (
    select
      se.*
    from {{ ref('sat_payment_financials_v0') }} se
)

select
  p.payment_id,
  o.order_id,
  pr.provider_id,

  se.payment_date        as payment_datetime,
  date(se.payment_date)  as payment_date,
  se.posting_date,

  sf.copay_amount,
  sf.insurance_amount,
  sf.total_amount,
  sf.cost_amount,
  sf.profit_amount,

  se.payer_type,
  se.payment_method,
  se.status
from payment p
left join sat_fin_current sf
  on sf.hk_payment_h = p.hk_payment_h
left join sat_evt_current se
  on se.hk_payment_h = p.hk_payment_h
left join link_payment_order lpo
  on lpo.hk_payment_h = p.hk_payment_h
left join "order" o
  on o.hk_order_h = lpo.hk_order_h
left join link_order_provider lop
  on lop.hk_order_h = o.hk_order_h
left join provider pr
  on pr.hk_provider_h = lop.hk_provider_h
;
