{{ config(
    materialized = 'table'
) }}

/*
    Revenue KPIs per reporting period (Day, Month, Year).

    Grain:
      - reporting_period ∈ { 'Day', 'Month', 'Year' }
      - reporting_date   (anchor date for that period)

    Metrics:
      - total_revenue
      - total_orders

    Window metrics (business sense):
      - prev_total_revenue / next_total_revenue:
          previous / next period for the same reporting_period
      - revenue_change_vs_prev / revenue_pct_change_vs_prev
      - rolling_7_period and rolling_30_period sums (e.g. last 7 days, last 7 months)
*/

with daily as (

    -- Intermediate layer already has daily revenue per order_date
    select
        order_date
      , total_revenue
      , total_orders
    from {{ ref('int_revenue_daily') }}

)

, day_level as (

    select
        'Day'          as reporting_period
      , daily.order_date as reporting_date
      , daily.total_revenue
      , daily.total_orders
    from daily

)

, month_level as (

    select
        'Month'                                      as reporting_period
      , date_trunc(daily.order_date, month)         as reporting_date
      , sum(daily.total_revenue)                    as total_revenue
      , sum(daily.total_orders)                     as total_orders
    from daily
    group by
        reporting_period
      , reporting_date

)

, year_level as (

    select
        'Year'                                       as reporting_period
      , date_trunc(daily.order_date, year)          as reporting_date
      , sum(daily.total_revenue)                    as total_revenue
      , sum(daily.total_orders)                     as total_orders
    from daily
    group by
        reporting_period
      , reporting_date

)

, revenue_per_period as (

    -- Union all grains into a single table
    select
        reporting_period
      , reporting_date
      , total_revenue
      , total_orders
    from day_level

    union all

    select
        reporting_period
      , reporting_date
      , total_revenue
      , total_orders
    from month_level

    union all

    select
        reporting_period
      , reporting_date
      , total_revenue
      , total_orders
    from year_level

)

, joined_with_calendar as (

    select
        rpp.reporting_period
      , rpp.reporting_date

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

      , rpp.total_revenue
      , rpp.total_orders
    from revenue_per_period as rpp
    left join {{ ref('dim_reporting_periods') }} as cal
        on  cal.reporting_period = rpp.reporting_period
        and cal.reporting_date   = rpp.reporting_date

)

select
    reporting_period
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

  , total_revenue
  , total_orders

  -- Previous / next period *for this reporting_period as a whole*
  , lag(total_revenue) over (
        partition by reporting_period
        order by reporting_date
    )                                           as prev_total_revenue

  , lead(total_revenue) over (
        partition by reporting_period
        order by reporting_date
    )                                           as next_total_revenue

  , total_revenue
    - lag(total_revenue) over (
          partition by reporting_period
          order by reporting_date
      )                                         as revenue_change_vs_prev

  , case
        when lag(total_revenue) over (
                 partition by reporting_period
                 order by reporting_date
             ) is null
             or lag(total_revenue) over (
                 partition by reporting_period
                 order by reporting_date
             ) = 0
            then null
        else
            ( total_revenue
            - lag(total_revenue) over (
                  partition by reporting_period
                  order by reporting_date
              )
            )
            / lag(total_revenue) over (
                  partition by reporting_period
                  order by reporting_date
              )
    end                                         as revenue_pct_change_vs_prev

  -- Rolling sums over the last 7 and 30 periods
  , sum(total_revenue) over (
        partition by reporting_period
        order by reporting_date
        rows between 6 preceding and current row
    )                                           as revenue_last_7_periods

  , sum(total_revenue) over (
        partition by reporting_period
        order by reporting_date
        rows between 29 preceding and current row
    )                                           as revenue_last_30_periods

from joined_with_calendar
order by
    reporting_period
  , reporting_date