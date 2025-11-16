{{ config(
    materialized='table'
) }}

/*
    Intermediate model aggregating product performance metrics. It joins
    products to order line items to compute revenue, quantity sold and
    order counts for each product. Products without sales will still
    appear with zero metrics. A revenue rank across products is added
    using window functions.
*/

with products as (
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
        products.product_id
      , products.product_shop_id as shop_id
      , coalesce(sum(order_line_items.line_total), 0) as total_revenue
      , coalesce(sum(order_line_items.quantity), 0) as total_quantity
      , count(distinct order_line_items.order_id) as total_orders
    from products
    left join order_line_items
        on products.product_id = order_line_items.product_id
    group by 1, 2
)

select
    aggregated.product_id
  , aggregated.shop_id
  , aggregated.total_revenue
  , aggregated.total_quantity
  , aggregated.total_orders
  , dense_rank() over(order by aggregated.total_revenue desc) as product_revenue_rank
from aggregated