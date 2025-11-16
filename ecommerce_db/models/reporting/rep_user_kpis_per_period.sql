{{ config(
    materialized = 'table'
) }}

/*
    User KPIs per reporting period (Day, Month, Year).

    Grain:
      - user_id
      - reporting_period ∈ { 'Day', 'Month', 'Year' }
      - reporting_date   (anchor date for that period)

    Metrics:
      - total_orders
      - total_revenue
      - avg_order_value

    Window metrics (business sense):
      - prev_total_revenue / next_total_revenue: previous/next period for
        the same user and reporting_period (e.g. previous month for that user)
      - revenue_change_vs_prev / revenue_pct_change_vs_prev

    Calendar attributes come from dim_reporting_periods.
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

    -- Per-order revenue per day
    select
        orders.order_id
      , orders.user_id
      , orders.order_date
      , coalesce(sum(order_line_items.line_total), 0) as order_revenue_amount
    from orders
    left join order_line_items
        on orders.order_id = order_line_items.order_id
    group by
        orders.order_id
      , orders.user_id
      , orders.order_date

)

, day_level as (

    -- KPIs per user per day
    select
        'Day'                         as reporting_period
      , order_revenue.order_date      as reporting_date
      , order_revenue.user_id
      , count(distinct order_revenue.order_id)      as total_orders
      , sum(order_revenue.order_revenue_amount)     as total_revenue
    from order_revenue
    group by
        reporting_period
      , reporting_date
      , user_id

)

, month_level as (

    -- KPIs per user per month
    select
        'Month'                           as reporting_period
      , date_trunc(order_revenue.order_date, month) as reporting_date
      , order_revenue.user_id
      , count(distinct order_revenue.order_id)      as total_orders
      , sum(order_revenue.order_revenue_amount)     as total_revenue
    from order_revenue
    group by
        reporting_period
      , reporting_date
      , user_id

)

, year_level as (

    -- KPIs per user per year
    select
        'Year'                          as reporting_period
      , date_trunc(order_revenue.order_date, year) as reporting_date
      , order_revenue.user_id
      , count(distinct order_revenue.order_id)      as total_orders
      , sum(order_revenue.order_revenue_amount)     as total_revenue
    from order_revenue
    group by
        reporting_period
      , reporting_date
      , user_id

)

, user_kpis_per_period as (

    select
        reporting_period
      , reporting_date
      , user_id
      , total_orders
      , total_revenue
      , case
            when total_orders > 0
                then total_revenue / total_orders
            else null
        end as avg_order_value
    from day_level

    union all

    select
        reporting_period
      , reporting_date
      , user_id
      , total_orders
      , total_revenue
      , case
            when total_orders > 0
                then total_revenue / total_orders
            else null
        end as avg_order_value
    from month_level

    union all

    select
        reporting_period
      , reporting_date
      , user_id
      , total_orders
      , total_revenue
      , case
            when total_orders > 0
                then total_revenue / total_orders
            else null
        end as avg_order_value
    from year_level

)

, joined_with_calendar as (

    select
        kpis.user_id
      , kpis.reporting_period
      , kpis.reporting_date

      -- Calendar attributes from the dim
      , cal.calendar_year
      , cal.calendar_quarter
      , cal.calendar_month
      , cal.calendar_month_name
      , cal.iso_week
      , cal.day_of_week
      , cal.day_name
      , cal.is_weekend
      , cal.is_greek_public_holiday
      , cal.greek_public_holiday_name

      , kpis.total_orders
      , kpis.total_revenue
      , kpis.avg_order_value

    from user_kpis_per_period as kpis
    left join {{ ref('dim_reporting_periods') }} as cal
        on  cal.reporting_period = kpis.reporting_period
        and cal.reporting_date   = kpis.reporting_date

)

select
    user_id
  , reporting_period
  , reporting_date

  , calendar_year
  , calendar_quarter
  , calendar_month
  , calendar_month_name
  , iso_week
  , day_of_week
  , day_name
  , is_weekend
  , is_greek_public_holiday
  , greek_public_holiday_name

  , total_orders
  , total_revenue
  , avg_order_value

  -- Business-sense window calcs:
  -- previous / next period for the same user & reporting_period
  , lag(total_revenue) over (
        partition by user_id, reporting_period
        order by reporting_date
    ) as prev_total_revenue

  , lead(total_revenue) over (
        partition by user_id, reporting_period
        order by reporting_date
    ) as next_total_revenue

  , total_revenue
    - lag(total_revenue) over (
          partition by user_id, reporting_period
          order by reporting_date
      ) as revenue_change_vs_prev

  , case
        when lag(total_revenue) over (
                 partition by user_id, reporting_period
                 order by reporting_date
             ) = 0
             or lag(total_revenue) over (
                 partition by user_id, reporting_period
                 order by reporting_date
             ) is null
            then null
        else
            ( total_revenue
            - lag(total_revenue) over (
                  partition by user_id, reporting_period
                  order by reporting_date
              )
            )
            / lag(total_revenue) over (
                  partition by user_id, reporting_period
                  order by reporting_date
              )
    end as revenue_pct_change_vs_prev

from joined_with_calendar
order by
    user_id
  , reporting_period
  , reporting_date
