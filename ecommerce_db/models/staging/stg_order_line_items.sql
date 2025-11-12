{{ config(materialized='view') }}

select
    id as order_line_items_id
    , order_id
    , product_id
    , sku_id
    , quantity
    , unit_price
    , line_total
from {{ source('pg', 'order_line_items') }}
