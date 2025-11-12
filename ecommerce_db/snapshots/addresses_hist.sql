{% snapshot addresses_hist %}
{{ config(
    target_schema='historical',
    unique_key='id',
    strategy='check',
    check_cols=['country','region','city','postal_code','address_line1','address_line2','latitude','longitude'],
    invalidate_hard_deletes=True
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
  created_at
from {{ source('pg', 'addresses') }}

{% endsnapshot %}
