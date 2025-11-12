{{ config(materialized='view') }}

select
    user_id
    , address_id
    , label as user_address_label
    , is_default_shipping
    , is_default_billing
    , created_at as user_address_created_at
from {{ source('pg', 'users_addresses') }}
