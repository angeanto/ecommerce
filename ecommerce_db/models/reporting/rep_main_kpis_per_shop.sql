{{ config(
    materialized='table'
) }}

with revenue_per_shop as (
    select
        *
    from {{ ref('int_revenue_per_shop') }}
),

products_per_shop as (
    select
        *
    from {{ ref('int_products_per_shop') }}
),

joined as (
    select
        revenue_per_shop.shop_id
      , revenue_per_shop.shop_name
      , revenue_per_shop.total_revenue
      , revenue_per_shop.revenue_rank
      , products_per_shop.total_products
      , products_per_shop.product_count_rank
    from revenue_per_shop
    left join products_per_shop
        on revenue_per_shop.shop_id = products_per_shop.shop_id
),

enhanced as (
    select
        *
      , lag(total_revenue) over(order by total_revenue desc) as previous_shop_revenue
      , lead(total_revenue) over(order by total_revenue desc) as next_shop_revenue
      , sum(total_revenue) over(order by total_revenue desc rows between unbounded preceding and current row) as cumulative_revenue
    from joined
)

select
    shop_id
  , shop_name
  , total_revenue
  , revenue_rank
  , total_products
  , product_count_rank
  , previous_shop_revenue
  , next_shop_revenue
  , cumulative_revenue
from enhanced