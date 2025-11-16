{{ config(
    materialized = 'view'
) }}

/*
    Staging model generating a dense calendar of dates.

    Grain:
      - one row per calendar date in the configured range
*/

with date_range as (

    select
        date '2015-01-01' as start_date
      , date '2030-12-31' as end_date

)

select
    date_add(
        (select start_date from date_range)
      , interval day_offset day
    ) as date_column
from unnest(
    generate_array(
          0
        , date_diff(
              (select end_date from date_range)
            , (select start_date from date_range)
            , day
          )
    )
) as day_offset