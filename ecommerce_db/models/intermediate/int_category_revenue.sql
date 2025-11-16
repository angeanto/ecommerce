{{ config(
    materialized='table'
) }}

/*
    Intermediate model computing sales performance metrics by category.
    It aggregates line item revenue and quantity across all products
    belonging to each category. Categories with no associated sales
    still appear with zero metrics due to the outer joins from
    categories down to order_line_items. A revenue rank across categories
    is included using window functions.
*/

with categories as (
    select
        *
    from {{ ref('stg_categories') }}
),

skus as (
    select
        *
    from {{ ref('stg_skus') }}
),

products as (
    select
        *
    from {{ ref('stg_products') }}
),

order_line_items as (
    select
        *
    from {{ ref('stg_order_line_items') }}
),

aggregated as (
    select
        categories.category_id
      , categories.category_name
      , coalesce(sum(order_line_items.line_total), 0) as total_revenue
      , coalesce(sum(order_line_items.quantity), 0) as total_quantity
      , count(distinct order_line_items.order_id) as total_orders
    from categories
    left join skus
        on categories.category_id = skus.category_id
    left join products
        on skus.sku_id = products.sku_id
    left join order_line_items
        on products.product_id = order_line_items.product_id
    group by 1, 2
)

select
    aggregated.category_id
  , aggregated.category_name
  , aggregated.total_revenue
  , aggregated.total_quantity
  , aggregated.total_orders
  , dense_rank() over(order by aggregated.total_revenue desc) as category_revenue_rank
from aggregated