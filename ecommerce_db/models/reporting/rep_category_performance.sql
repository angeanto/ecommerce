{{ config(
    materialized='table'
) }}

/*
    Reporting model that enriches category performance metrics with
    dimensional attributes from the categories staging model. It
    includes the category slug and created_at timestamp alongside
    revenue, quantity and order counts. Window functions compute
    business‑focused analytics such as revenue share, cumulative revenue
    share (for Pareto analysis), differences in revenue to the previous
    category in the ranking, percentage differences, a revenue index
    relative to the average category, and cumulative revenue across
    ranked categories. These measures help identify top‑performing
    categories and the distribution of sales across the assortment.
*/

with category_metrics as (
    select
        *
    from {{ ref('int_category_revenue') }}
),

categories as (
    select
        *
    from {{ ref('stg_categories') }}
)

select
    categories.category_id
  , categories.category_name
  , categories.category_slug
  , categories.category_created_at
  , category_metrics.total_revenue
  , category_metrics.total_quantity
  , category_metrics.total_orders
  , category_metrics.category_revenue_rank
  -- share of total revenue contributed by this category across all categories
  , category_metrics.total_revenue
      / nullif(sum(category_metrics.total_revenue) over(), 0) as revenue_share
  -- cumulative revenue share when categories are ranked by revenue (Pareto analysis)
  , sum(category_metrics.total_revenue)
      over(order by category_metrics.total_revenue desc
           rows between unbounded preceding and current row)
      / nullif(sum(category_metrics.total_revenue) over(), 0) as cumulative_revenue_share
  -- difference in revenue compared to the previous category in the ranking
  , category_metrics.total_revenue
      - lag(category_metrics.total_revenue)
          over(order by category_metrics.total_revenue desc) as revenue_diff_vs_prev
  -- percent difference in revenue compared to the previous category
  , case
        when lag(category_metrics.total_revenue)
          over(order by category_metrics.total_revenue desc) = 0
        then null
        else (category_metrics.total_revenue
                / lag(category_metrics.total_revenue)
                    over(order by category_metrics.total_revenue desc)) - 1
    end as revenue_pct_diff_vs_prev
  -- index against average category revenue (values > 1 indicate above‑average performance)
  , category_metrics.total_revenue
      / nullif(avg(category_metrics.total_revenue) over(), 0) as revenue_index
  -- cumulative revenue across categories (not share but raw amount)
  , sum(category_metrics.total_revenue)
      over(order by category_metrics.total_revenue desc
           rows between unbounded preceding and current row) as cumulative_category_revenue
from categories
left join category_metrics
    on categories.category_id = category_metrics.category_id