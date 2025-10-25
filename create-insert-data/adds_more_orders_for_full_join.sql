begin;

set search_path to app, public;

-- 1) ensure two skus exist (active & sold) and (sold but inactive)
with
cat as (
  select categories.id as category_id
  from categories
  order by categories.id
  limit 1
),
ins as (
  insert into skus (sku_code, title, brand, category_id, attributes, created_at)
  select
    'SKU-ACTIVE-SOLD', 'Demo Product Active & Sold', 'BrandX',
    cat.category_id,
    jsonb_build_object('color','black','weight_kg',1.5,'warranty_months',12),
    now()
  from cat
  on conflict (sku_code) do nothing
  returning skus.id, skus.sku_code
),
ins2 as (
  insert into skus (sku_code, title, brand, category_id, attributes, created_at)
  select
    'SKU-SOLD-INACTIVE', 'Demo Product Sold but Inactive', 'BrandY',
    cat.category_id,
    jsonb_build_object('color','red','weight_kg',1.2,'warranty_months',24),
    now()
  from cat
  on conflict (sku_code) do nothing
  returning skus.id, skus.sku_code
)
select 1;

-- 2) ensure products (offers) exist for those skus
with
shop_first as (
  select shops.id as shop_id
  from shops
  order by shops.id
  limit 1
),
shop_last as (
  select shops.id as shop_id
  from shops
  order by shops.id desc
  limit 1
),
sku_ids as (
  select skus.id, skus.sku_code
  from skus
  where skus.sku_code in ('SKU-ACTIVE-SOLD','SKU-SOLD-INACTIVE')
),
ins_offers as (
  -- active & sold offer (active=true) on first shop
  insert into products (shop_id, sku_id, price, currency, stock, condition, is_active, created_at)
  select
    shop_first.shop_id,
    (select sku_ids.id from sku_ids where sku_ids.sku_code = 'SKU-ACTIVE-SOLD'),
    199.99, 'EUR', 100, 'new'::app.product_condition, true, now()
  from shop_first
  on conflict (shop_id, sku_id) do nothing
  returning products.id
),
ins_offers2 as (
  -- sold but inactive offer (active=false) on last shop
  insert into products (shop_id, sku_id, price, currency, stock, condition, is_active, created_at)
  select
    shop_last.shop_id,
    (select sku_ids.id from sku_ids where sku_ids.sku_code = 'SKU-SOLD-INACTIVE'),
    299.99, 'EUR', 50, 'new'::app.product_condition, false, now()
  from shop_last
  on conflict (shop_id, sku_id) do nothing
  returning products.id
)
select 1;

-- 3) create two orders for an existing user with addresses
with
u as (
  select users.id as user_id
  from users
  order by users.id
  limit 1
),
addr as (
  select users_addresses.address_id
  from users_addresses
  where users_addresses.user_id = (select u.user_id from u)
  order by users_addresses.address_id
  limit 1
),
offer_rows as (
  select products.id as product_id, products.sku_id, products.price, skus.sku_code
  from products
  inner join skus
    on skus.id = products.sku_id
  where skus.sku_code in ('SKU-ACTIVE-SOLD','SKU-SOLD-INACTIVE')
  order by products.id
),
ins_orders as (
  insert into orders (user_id, shipping_address_id, billing_address_id, status, total_amount, currency, created_at, updated_at)
  select
    (select u.user_id from u),
    (select addr.address_id from addr),
    (select addr.address_id from addr),
    'paid'::app.order_status,
    0,
    'EUR',
    now() - interval '1 day',
    now()
  from offer_rows
  limit 2
  returning orders.id
),
ord_enumerated as (
  select orders.id as order_id, row_number() over (order by orders.id desc) as rn
  from orders
  order by orders.id desc
  limit 2
),
prod_enumerated as (
  select offer_rows.product_id, offer_rows.sku_id, offer_rows.price, row_number() over (order by offer_rows.product_id) as rn
  from offer_rows
)
insert into order_line_items (order_id, product_id, sku_id, quantity, unit_price)
select
  ord_enumerated.order_id,
  prod_enumerated.product_id,
  prod_enumerated.sku_id,
  1,
  prod_enumerated.price
from ord_enumerated
inner join prod_enumerated
  on ord_enumerated.rn = prod_enumerated.rn;

-- 4) recalc totals and create payments, link back
with
totals as (
  select order_line_items.order_id, sum(order_line_items.line_total) as sum_total
  from order_line_items
  group by order_line_items.order_id
),
upd as (
  update orders
  set total_amount = totals.sum_total,
      updated_at = now()
  from totals
  where orders.id = totals.order_id
  returning orders.id, orders.total_amount
),
ins_pay as (
  insert into payments (provider, status, amount, currency, external_ref, created_at)
  select
    'card'::app.payment_provider,
    'captured'::app.payment_status,
    upd.total_amount,
    'EUR',
    'TX-DEMO-' || upd.id::text,
    now()
  from upd
  returning payments.id, payments.external_ref, payments.created_at
)
update orders
set payment_id = payments.id,
    updated_at = greatest(orders.updated_at, payments.created_at)
from payments
where payments.external_ref = ('TX-DEMO-' || orders.id::text);

commit;
