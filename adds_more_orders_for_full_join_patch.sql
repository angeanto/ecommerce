begin;

set search_path to app, public;

-- 1) ensure demo skus exist
with
cat as (
  select categories.id as category_id
  from categories
  order by categories.id
  limit 1
),
ins1 as (
  insert into skus (sku_code, title, brand, category_id, attributes, created_at)
  select
    'SKU-ACTIVE-SOLD',
    'Demo Product Active & Sold',
    'BrandX',
    cat.category_id,
    jsonb_build_object('color','black','weight_kg',1.5,'warranty_months',12),
    now()
  from cat
  on conflict (sku_code) do nothing
  returning skus.id
),
ins2 as (
  insert into skus (sku_code, title, brand, category_id, attributes, created_at)
  select
    'SKU-SOLD-INACTIVE',
    'Demo Product Sold but Inactive',
    'BrandY',
    cat.category_id,
    jsonb_build_object('color','red','weight_kg',1.2,'warranty_months',24),
    now()
  from cat
  on conflict (sku_code) do nothing
  returning skus.id
)
select 1;

-- 2) ensure a shop for each case (first and last by id, deterministic)
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
sku_active as (
  select skus.id as sku_id
  from skus
  where skus.sku_code = 'SKU-ACTIVE-SOLD'
),
sku_inactive as (
  select skus.id as sku_id
  from skus
  where skus.sku_code = 'SKU-SOLD-INACTIVE'
),
-- ensure one active offer for SKU-ACTIVE-SOLD
ins_active_offer as (
  insert into products (shop_id, sku_id, price, currency, stock, condition, is_active, created_at)
  select
    shop_first.shop_id,
    sku_active.sku_id,
    199.99,
    'EUR',
    100,
    'new'::app.product_condition,
    true,
    now()
  from shop_first, sku_active
  on conflict (shop_id, sku_id) do nothing
  returning products.id
),
-- if offer exists but inactive, flip to active
upd_active_offer as (
  update products
  set is_active = true
  from sku_active, shop_first
  where products.sku_id = sku_active.sku_id
    and products.shop_id = shop_first.shop_id
    and products.is_active = false
  returning products.id
),
-- ensure at least one offer exists for SKU-SOLD-INACTIVE (may be inserted now)
ins_inactive_offer as (
  insert into products (shop_id, sku_id, price, currency, stock, condition, is_active, created_at)
  select
    shop_last.shop_id,
    sku_inactive.sku_id,
    299.99,
    'EUR',
    50,
    'new'::app.product_condition,
    false,
    now()
  from shop_last, sku_inactive
  on conflict (shop_id, sku_id) do nothing
  returning products.id
),
-- force ALL offers for SKU-SOLD-INACTIVE to be inactive (critical for the classification)
upd_inactivate_all as (
  update products
  set is_active = false
  from sku_inactive
  where products.sku_id = sku_inactive.sku_id
    and products.is_active = true
  returning products.id
)
select 1;

-- 3) pick a user + address deterministically
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
p_active as (
  select products.id as product_id, products.sku_id, products.price
  from products
  inner join skus
    on skus.id = products.sku_id
  where skus.sku_code = 'SKU-ACTIVE-SOLD'
    and products.is_active = true
  order by products.id
  limit 1
),
p_inactive as (
  select products.id as product_id, products.sku_id, products.price
  from products
  inner join skus
    on skus.id = products.sku_id
  where skus.sku_code = 'SKU-SOLD-INACTIVE'
  order by products.id
  limit 1
),
-- create two paid orders (one per case)
o1 as (
  insert into orders (user_id, shipping_address_id, billing_address_id, status, total_amount, currency, created_at, updated_at)
  select
    (select u.user_id from u),
    (select addr.address_id from addr),
    (select addr.address_id from addr),
    'paid'::app.order_status,
    0,
    'EUR',
    now() - interval '2 days',
    now()
  from p_active
  on conflict do nothing
  returning orders.id
),
o2 as (
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
  from p_inactive
  on conflict do nothing
  returning orders.id
),
-- if the above "on conflict do nothing" returns nothing (rare), pick last two orders of this user to proceed
choose_o1 as (
  select orders.id as order_id
  from orders
  where orders.user_id = (select u.user_id from u)
  order by orders.id desc
  limit 1
),
choose_o2 as (
  select orders.id as order_id
  from orders
  where orders.user_id = (select u.user_id from u)
  order by orders.id desc
  offset 1
  limit 1
),
final_o1 as (
  select coalesce((select o1.id from o1), (select choose_o1.order_id from choose_o1)) as order_id
),
final_o2 as (
  select coalesce((select o2.id from o2), (select choose_o2.order_id from choose_o2)) as order_id
),
-- insert line items for those two orders
ins_li_1 as (
  insert into order_line_items (order_id, product_id, sku_id, quantity, unit_price)
  select
    (select final_o1.order_id from final_o1),
    p_active.product_id,
    p_active.sku_id,
    1,
    p_active.price
  from p_active
  on conflict do nothing
  returning order_line_items.id
),
ins_li_2 as (
  insert into order_line_items (order_id, product_id, sku_id, quantity, unit_price)
  select
    (select final_o2.order_id from final_o2),
    p_inactive.product_id,
    p_inactive.sku_id,
    1,
    p_inactive.price
  from p_inactive
  on conflict do nothing
  returning order_line_items.id
),
-- recalc totals for those two orders
totals as (
  select order_line_items.order_id, sum(order_line_items.line_total) as sum_total
  from order_line_items
  where order_line_items.order_id in ((select final_o1.order_id from final_o1), (select final_o2.order_id from final_o2))
  group by order_line_items.order_id
),
upd_orders as (
  update orders
  set total_amount = totals.sum_total,
      updated_at = now()
  from totals
  where orders.id = totals.order_id
  returning orders.id, orders.total_amount
),
-- payments: captured for both
ins_pay as (
  insert into payments (provider, status, amount, currency, external_ref, created_at)
  select
    'card'::app.payment_provider,
    'captured'::app.payment_status,
    upd_orders.total_amount,
    'EUR',
    'TX-DEMO-FORCE-' || upd_orders.id::text,
    now()
  from upd_orders
  on conflict do nothing
  returning payments.id, payments.external_ref, payments.created_at
)
update orders
set payment_id = payments.id,
    updated_at = greatest(orders.updated_at, payments.created_at)
from payments
where payments.external_ref = ('TX-DEMO-FORCE-' || orders.id::text);

commit;