{{ config(materialized='view') }}

select
    id as product_id
    , shop_id as product_shop_id
    , sku_id
    , price as product_price
    , currency
    , stock as product_stock
    , condition as product_condition
    , is_active as product_is_active
    , created_at as product_created_at
from {{ source('pg', 'products') }}
