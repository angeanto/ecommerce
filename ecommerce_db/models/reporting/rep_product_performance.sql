{{ config(
    materialized='table'
) }}

/*
    Reporting model enriching product performance metrics with
    dimensional attributes such as product details, SKU attributes,
    category information and shop context. This model enables
    comprehensive analysis of how individual products perform in
    relation to their descriptive attributes. Window functions
    add lag/lead and cumulative revenue analytics across products.
*/

with product_perf as (
    select
        *
    from {{ ref('int_product_performance') }}
),

products as (
    select
        *
    from {{ ref('stg_products') }}
),

skus as (
    select
        *
    from {{ ref('stg_skus') }}
),

categories as (
    select
        *
    from {{ ref('stg_categories') }}
),

shops as (
    select
        *
    from {{ ref('stg_shops') }}
)

select
    product_perf.product_id
  , skus.sku_title
  , skus.brand
  , categories.category_name
  , product_perf.shop_id as shop_id
  , shops.shop_name
  , products.product_price
  , products.currency
  , products.product_stock
  , products.product_condition
  , products.product_is_active
  , product_perf.total_quantity
  , product_perf.total_revenue
  , product_perf.total_orders
  , product_perf.product_revenue_rank
  , lag(product_perf.total_revenue) over(order by product_perf.total_revenue desc) as previous_product_revenue
  , lead(product_perf.total_revenue) over(order by product_perf.total_revenue desc) as next_product_revenue
  , sum(product_perf.total_revenue) over(order by product_perf.total_revenue desc rows between unbounded preceding and current row) as cumulative_product_revenue
from product_perf
left join products
    on product_perf.product_id = products.product_id
left join skus
    on products.sku_id = skus.sku_id
left join categories
    on skus.category_id = categories.category_id
left join shops
    on product_perf.shop_id = shops.shop_id