{{ config(
    materialized='table'
) }}

with products as (
    select
        *
    from {{ ref('stg_products') }}
),

shops as (
    select
        *
    from {{ ref('stg_shops') }}
),

aggregated as (
    select
        shops.shop_id
      , shops.shop_name
      , count(distinct product_id) as total_products
    from shops
    left join products
        on shops.shop_id = products.product_shop_id
    group by 1, 2
)

select
    aggregated.shop_id
  , aggregated.shop_name
  , aggregated.total_products
  , dense_rank() over(order by aggregated.total_products desc) as product_count_rank
from aggregated
