{{ config(
    materialized='table'
) }}

with products as (
    select *
    from {{ ref('stg_products') }}
)

, shops as (
    select *
    from {{ ref('stg_shops') }}
)

, joined as (
    select
    shops.shop_id
    , shops.shop_name
    , count(distinct(product_id)) as total_products
    from shops
    left join products on shops.shop_id = products.product_shop_id
    group by 1,2
)

select * from joined
