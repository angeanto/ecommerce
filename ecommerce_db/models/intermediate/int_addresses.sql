{{ config(
    materialized='table'
) }}

with base as (
    select *
    from {{ ref('stg_addresses') }}
)

select * from base
