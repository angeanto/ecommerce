{{ config(
    materialized = 'table'
) }}

/*
    Dimension of reporting periods.

    Grain:
      - reporting_period ∈ { 'Day', 'Week', 'Month', 'Quarter', 'Year' }
      - reporting_date   (anchor date for that period)

    Source:
      - stg_all_dates (dense series of calendar dates)

    Enrichment:
      - calendar_year, calendar_quarter, calendar_month(+name), iso_week
      - day_of_week, day_name
      - is_weekend
      - is_greek_public_holiday, greek_public_holiday_name

    Recommended tests (in a .yml file):
      - not_null: [reporting_period, reporting_date]
      - unique: [reporting_period, reporting_date]
*/

with all_dates as (

    select
        date_column
    from {{ ref('stg_all_dates') }}

)

, processed_dates as (

    -- Equivalent of your reporting_periods_table
    select
        'Day'                        as reporting_period
      , date_trunc(date_column, day) as reporting_date
    from all_dates
    group by
        reporting_period
      , reporting_date

    union all

    select
        'Week'                         as reporting_period
      , date_trunc(date_column, week)  as reporting_date
    from all_dates
    group by
        reporting_period
      , reporting_date

    union all

    select
        'Month'                        as reporting_period
      , date_trunc(date_column, month) as reporting_date
    from all_dates
    group by
        reporting_period
      , reporting_date

    union all

    select
        'Quarter'                         as reporting_period
      , date_trunc(date_column, quarter)  as reporting_date
    from all_dates
    group by
        reporting_period
      , reporting_date

    union all

    select
        'Year'                        as reporting_period
      , date_trunc(date_column, year) as reporting_date
    from all_dates
    group by
        reporting_period
      , reporting_date

)

, filtered_periods as (

    -- Safeguard: never include future anchor dates
    select
        reporting_period
      , reporting_date
    from processed_dates
)

, enriched as (

    select
        reporting_period
      , reporting_date

      -- Calendar attributes
      , extract(year    from reporting_date)        as calendar_year
      , extract(quarter from reporting_date)        as calendar_quarter
      , extract(month   from reporting_date)        as calendar_month
      , format_date('%B', reporting_date)           as calendar_month_name
      , extract(isoweek from reporting_date)        as iso_week
      , extract(dayofweek from reporting_date)      as day_of_week  -- 1=Sunday ... 7=Saturday
      , format_date('%A', reporting_date)           as day_name

      -- Weekend flag
      , case
            when extract(dayofweek from reporting_date) in (1, 7)
                then true
            else false
        end                                         as is_weekend

      -- Fixed-date Greek public holidays
      , case
            when extract(month from reporting_date) = 1 and extract(day from reporting_date) = 1  then true -- New Year
            when extract(month from reporting_date) = 1 and extract(day from reporting_date) = 6  then true -- Epiphany
            when extract(month from reporting_date) = 3 and extract(day from reporting_date) = 25 then true -- Independence Day / Annunciation
            when extract(month from reporting_date) = 5 and extract(day from reporting_date) = 1  then true -- Labour Day
            when extract(month from reporting_date) = 8 and extract(day from reporting_date) = 15 then true -- Dormition
            when extract(month from reporting_date) = 10 and extract(day from reporting_date) = 28 then true -- Ochi Day
            when extract(month from reporting_date) = 12 and extract(day from reporting_date) = 25 then true -- Christmas Day
            when extract(month from reporting_date) = 12 and extract(day from reporting_date) = 26 then true -- Synaxis of the Mother of God
            else false
        end                                         as is_greek_public_holiday

      , case
            when extract(month from reporting_date) = 1 and extract(day from reporting_date) = 1  then 'New Years Day'
            when extract(month from reporting_date) = 1 and extract(day from reporting_date) = 6  then 'Epiphany'
            when extract(month from reporting_date) = 3 and extract(day from reporting_date) = 25 then 'Independence Day / Annunciation'
            when extract(month from reporting_date) = 5 and extract(day from reporting_date) = 1  then 'Labour Day'
            when extract(month from reporting_date) = 8 and extract(day from reporting_date) = 15 then 'Dormition of the Mother of God'
            when extract(month from reporting_date) = 10 and extract(day from reporting_date) = 28 then 'Ochi Day'
            when extract(month from reporting_date) = 12 and extract(day from reporting_date) = 25 then 'Christmas Day'
            when extract(month from reporting_date) = 12 and extract(day from reporting_date) = 26 then 'Synaxis of the Mother of God'
            else null
        end                                         as greek_public_holiday_name
from filtered_periods
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
from enriched
order by
    reporting_date
  , reporting_period
