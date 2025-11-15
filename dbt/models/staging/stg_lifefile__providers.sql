{{ config(materialized = 'view') }}

with src as (

    select
        *
    from {{ source('lifefile', 'providers_raw') }}

),

normalized as (

    select
        -- business key
        provider_id,

        -- identifiers
        nullif(trim(npi), '')                         as npi,

        -- basic profile
        nullif(trim(first_name), '')                  as first_name,
        nullif(trim(last_name), '')                   as last_name,

        -- location
        nullif(trim(city), '')                        as city,
        nullif(trim(state), '')                       as state,
  
        case
            when email is not null and trim(email) <> '' then
                to_hex(sha256(to_utf8(lower(trim(email)))))
            else null
        end                                           as email_sha256,

        case
            when phone is not null and trim(phone) <> '' then
                to_hex(
                    sha256(
                        to_utf8(
                            regexp_replace(phone, r'[^0-9]+', '')
                        )
                    )
                )
            else null
        end                                           as phone_sha256,

        -- normalize is_active to boolean
        cast(
            case
                when lower(cast(is_active as string)) in ('1', 't', 'true', 'y', 'yes') then true
                when lower(cast(is_active as string)) in ('0', 'f', 'false', 'n', 'no') then false
                else null
            end
            as bool
        )                                             as is_active,

        -- timestamps
        cast(created_at as timestamp)                 as created_at,
        cast(updated_at as timestamp)                 as updated_at

    from src
)

select *
from normalized;
