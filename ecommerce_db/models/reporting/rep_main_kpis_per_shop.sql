{{ config(
    materialized='table'
) }}

with revenue_per_shop as (
    select *
    from {{ ref('int_revenue_per_shop') }}
)

,products_per_shop as (
    select *
    from {{ ref('int_products_per_shop') }}
)

, joined as (
    select 
    revenue_per_shop.shop_id
    , revenue_per_shop.shop_name
    , revenue_per_shop.total_revenue
    , products_per_shop.total_products
    from revenue_per_shop
    left join products_per_shop on revenue_per_shop.shop_id = products_per_shop.shop_id
)

select * from joined