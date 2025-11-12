{{ config(
    materialized = 'view',
    schema='current'
) }}

select
  id,
  country,
  region,
  city,
  postal_code,
  address_line1,
  address_line2,
  latitude,
  longitude,
  created_at,
  dbt_valid_from as valid_from
from {{ ref('addresses_hist') }}
where dbt_valid_to is null
