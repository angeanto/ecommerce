--add orders for existing customers
SET search_path TO app;

with
params as (
  select 400::integer as num_new_orders
),

-- users that have at least one address, plus their default shipping/billing
users_with_addresses as (
  select
    users_addresses.user_id,
    max(users_addresses.address_id) filter (where users_addresses.is_default_shipping) as default_ship,
    max(users_addresses.address_id) filter (where users_addresses.is_default_billing)  as default_bill,
    min(users_addresses.address_id)                                              as any_address
  from users_addresses
  group by users_addresses.user_id
),

-- finalize shipping/billing (fallback to any address if default is null)
user_address_final as (
  select
    users_with_addresses.user_id,
    coalesce(users_with_addresses.default_ship, users_with_addresses.any_address) as shipping_address_id,
    coalesce(users_with_addresses.default_bill, users_with_addresses.any_address) as billing_address_id
  from users_with_addresses
),

-- enumerate eligible users for deterministic mapping
users_enumerated as (
  select
    user_address_final.user_id,
    user_address_final.shipping_address_id,
    user_address_final.billing_address_id,
    row_number() over (order by user_address_final.user_id) as rn
  from user_address_final
),

users_count as (
  select count(*) as cnt
  from users_enumerated
),

-- create a series of N new orders and map each row to a user deterministically (round-robin)
order_series as (
  select generate_series(1, (select params.num_new_orders from params)) as seq
),

order_plan as (
  select
    users_enumerated.user_id,
    users_enumerated.shipping_address_id,
    users_enumerated.billing_address_id,
    -- spread creation dates over the last ~60 days deterministically
    (now() - (( (order_series.seq % 60) )::text || ' days')::interval) as created_at
  from order_series
  inner join users_enumerated
    on users_enumerated.rn = ( ( (order_series.seq - 1) % (select users_count.cnt from users_count) ) + 1 )
),

-- insert the new orders in pending state; return new order ids
orders_inserted as (
  insert into orders (
    user_id,
    shipping_address_id,
    billing_address_id,
    status,
    total_amount,
    currency,
    payment_id,
    created_at,
    updated_at
  )
  select
    order_plan.user_id,
    order_plan.shipping_address_id,
    order_plan.billing_address_id,
    'pending'::order_status,
    0,
    'EUR',
    null,
    order_plan.created_at,
    now()
  from order_plan
  returning orders.id, orders.user_id, orders.created_at
),

-- active products universe with a deterministic row number
active_products as (
  select
    products.id as product_id,
    products.sku_id,
    products.price,
    products.shop_id,
    row_number() over (order by products.id) as prn
  from products
  where products.is_active
),

active_products_count as (
  select count(*) as cnt
  from active_products
),

-- decide how many line items per order (1..4) deterministically from order id
items_per_order as (
  select
    orders_inserted.id as order_id,
    (1 + (orders_inserted.id % 4)) as n_items
  from orders_inserted
),

-- one row per line item to be created, per order
items_seed as (
  select
    items_per_order.order_id,
    generate_series(1, items_per_order.n_items) as item_n
  from items_per_order
),

-- pick a product deterministically per (order_id, item_n) without lateral/random
-- map to a product row number, then join to active_products by prn
items_with_products as (
  select
    items_seed.order_id,
    active_products.product_id,
    active_products.sku_id,
    active_products.price as unit_price,
    -- quantity in 1..3, deterministic by order_id and item_n
    (1 + ((items_seed.order_id + items_seed.item_n) % 3)) as quantity
  from items_seed
  inner join active_products
    on active_products.prn =
       ( ( (items_seed.order_id * 31 + items_seed.item_n * 17) % (select active_products_count.cnt from active_products_count) ) + 1 )
),

-- insert the line items
line_items_inserted as (
  insert into order_line_items (
    order_id,
    product_id,
    sku_id,
    quantity,
    unit_price
  )
  select
    items_with_products.order_id,
    items_with_products.product_id,
    items_with_products.sku_id,
    items_with_products.quantity,
    items_with_products.unit_price
  from items_with_products
  returning order_line_items.id, order_line_items.order_id, order_line_items.line_total
),

-- compute totals for only the newly inserted orders
order_totals as (
  select
    line_items_inserted.order_id,
    sum(line_items_inserted.line_total) as sum_total
  from line_items_inserted
  group by line_items_inserted.order_id
),

-- update orders with computed totals
orders_updated as (
  update orders
  set total_amount = order_totals.sum_total,
      updated_at   = now()
  from order_totals
  where orders.id = order_totals.order_id
  returning orders.id, orders.total_amount, orders.created_at
),

-- decide payment provider and status deterministically from order id
payment_plan as (
  select
    orders_updated.id as order_id,
    case (orders_updated.id % 4)
      when 0 then 'card'::payment_provider
      when 1 then 'paypal'::payment_provider
      when 2 then 'bank_transfer'::payment_provider
      else 'cod'::payment_provider
    end as provider,
    case
      when (orders_updated.id % 100) < 80 then 'captured'::payment_status
      when (orders_updated.id % 100) < 90 then 'authorized'::payment_status
      when (orders_updated.id % 100) < 97 then 'failed'::payment_status
      else 'refunded'::payment_status
    end as status,
    orders_updated.total_amount as amount,
    'EUR' as currency,
    ('TX-' || orders_updated.id::text) as external_ref,
    (orders_updated.created_at + ((orders_updated.id % 3600)::text || ' seconds')::interval) as created_at
  from orders_updated
),

-- insert payments
payments_inserted as (
  insert into payments (
    provider,
    status,
    amount,
    currency,
    external_ref,
    created_at
  )
  select
    payment_plan.provider,
    payment_plan.status,
    payment_plan.amount,
    payment_plan.currency,
    payment_plan.external_ref,
    payment_plan.created_at
  from payment_plan
  returning payments.id, payments.external_ref, payments.status, payments.created_at
),

-- link payments back to orders and set order status from payment outcome
orders_final as (
  update orders
  set payment_id = payments_inserted.id,
      status = case
                 when payments_inserted.status = 'captured'   then 'paid'::order_status
                 when payments_inserted.status = 'authorized' then 'pending'::order_status
                 when payments_inserted.status in ('failed','cancelled') then 'cancelled'::order_status
                 when payments_inserted.status = 'refunded'   then 'refunded'::order_status
                 else orders.status
               end,
      updated_at = greatest(orders.updated_at, payments_inserted.created_at)
  from payments_inserted
  where payments_inserted.external_ref = ('TX-' || orders.id::text)
  returning orders.id
)

select count(*) as new_orders_created
from orders_final;