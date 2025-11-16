{{ config(
    materialized='table'
) }}

/*
    Intermediate model computing total revenue and order counts by day.
    Orders without line items are handled via a left join, ensuring
    their presence in the daily summary with zero revenue.
*/

with orders as (
    select
        order_id
      , cast(order_created_at as date) as order_date
    from {{ ref('stg_orders') }}
),

order_line_items as (
    select
        order_id
      , line_total
    from {{ ref('stg_order_line_items') }}
)

select
    orders.order_date
  , coalesce(sum(order_line_items.line_total), 0) as total_revenue
  , count(distinct orders.order_id) as total_orders
from orders
left join order_line_items
    on orders.order_id = order_line_items.order_id
group by 1