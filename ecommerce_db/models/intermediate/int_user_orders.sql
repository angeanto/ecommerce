{{ config(
    materialized = 'table'
) }}

/*
    Intermediate model summarising order metrics per user.
    Computes total orders, total revenue, average order value,
    first order date and last order date for each user.
    Additionally calculates a revenue rank across users and
    the overall average revenue, demonstrating window functions.
*/

with orders as (

    select
        order_id
      , user_id
      , cast(order_created_at as date) as order_date
    from {{ ref('stg_orders') }}

)

, order_line_items as (

    select
        order_id
      , line_total
    from {{ ref('stg_order_line_items') }}

)

, order_revenue as (

    -- Join orders with their line items to compute revenue per order
    select
        orders.order_id
      , orders.user_id
      , orders.order_date
      , coalesce(sum(order_line_items.line_total), 0) as order_revenue
    from orders
    left join order_line_items
        on orders.order_id = order_line_items.order_id
    group by
        orders.order_id
      , orders.user_id
      , orders.order_date

)

, aggregated as (

    select
        user_id
      , count(distinct order_id)      as total_orders
      , sum(order_revenue.order_revenue)           as total_revenue
      , min(order_date)              as first_order_date
      , max(order_date)              as last_order_date
    from order_revenue
    group by
        user_id

)

select
    aggregated.user_id
  , aggregated.total_orders
  , aggregated.total_revenue
  , case
        when aggregated.total_orders > 0
            then aggregated.total_revenue / aggregated.total_orders
        else null
    end                                       as avg_order_value
  , aggregated.first_order_date
  , aggregated.last_order_date
  , dense_rank() over (
        order by aggregated.total_revenue desc
    )                                         as revenue_rank
  , avg(aggregated.total_revenue) over ()     as avg_total_revenue_overall
from aggregated
