{{ config(materialized='view') }}

select
    id as user_id
    , email as user_email
    , full_name as user_full_name
    , phone as user_phone
    , status as user_status
    , created_at as user_created_at
    , updated_at as user_updated_at
from {{ source('pg', 'users') }}
