{{ config(materialized='view') }}

select
    id as order_id
    , user_id
    , shipping_address_id
    , billing_address_id
    , status as order_status
    , total_amount as order_total_amount
    , currency as order_currency
    , payment_id
    , created_at as order_created_at
    , updated_at as order_updated_at
from {{ source('pg', 'orders') }}
