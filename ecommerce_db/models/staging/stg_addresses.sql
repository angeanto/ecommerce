{{ config(materialized='view') }}

select
    id as address_id
    , country as address_country
    , region as address_region
    , city as address_city
    , postal_code as address_postal_code
    , address_line1
    , address_line2
    , latitude
    , longitude
    , created_at as address_created_at
from {{ source('pg', 'addresses') }}
