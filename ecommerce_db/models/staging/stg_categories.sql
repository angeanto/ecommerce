{{ config(materialized='view') }}

select
    id as category_id
    , parent_id
    , name as category_name
    , slug as category_slug
    , created_at as category_created_at
from {{ source('pg', 'categories') }}
