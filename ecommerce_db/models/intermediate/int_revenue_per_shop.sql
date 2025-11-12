{{ config(
    materialized='table'
) }}

with order_line_items as (
    select *
    from {{ ref('stg_order_line_items') }}
)
, products as (
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
    , sum(line_total) as total_revenue
    from shops
    left join products on shops.shop_id = products.product_shop_id
    left join order_line_items on order_line_items.product_id = products.product_id
    group by 1,2
)

select * from joined
