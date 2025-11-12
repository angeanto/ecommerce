{{ config(materialized='view') }}

select
    id as sku_id
    , sku_code
    , title as sku_title
    , brand
    , category_id
    , attributes as sku_attributes
    , created_at as sku_created_at
from {{ source('pg', 'skus') }}
