{{ config(materialized='view') }}

select
    id as payment_id
    , provider
    , status as payment_status
    , amount as payment_amount
    , currency as payment_currency
    , external_ref
    , created_at as payment_created_at
from {{ source('pg', 'payments') }}
