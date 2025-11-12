{{ config(materialized='view') }}

select
    id as shop_id
    , name as shop_name
    , slug as shop_slug
    , url as shop_url
    , rating as shop_rating
    , status as shop_status
    , created_at as shop_created_at
from {{ source('pg', 'shops') }}
