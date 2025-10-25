# Advanced SQL Answers (PostgreSQL) — E‑commerce Schema

All solutions obey the constraints:
- **CTEs only** (no nested subqueries).
- Business assumptions: monetized orders are those with `orders.status IN ('paid','shipped','delivered')`.
- Exercise 8 uses **GROUP BY** to replace `DISTINCT ON` while preserving tie‑breaking behavior.

---

## 1) Running Total (GMV)
```sql
with
params as (
  select date_trunc('month', now())::date as month_start
),
facts as (
  select
    date_trunc('day', orders.created_at)::date as dt,
    order_line_items.line_total as amount
  from orders
  inner join order_line_items
    on order_line_items.order_id = orders.id
  where orders.status in ('paid','shipped','delivered')
    and orders.created_at >= (select month_start from params)
    and orders.created_at < ((select month_start from params) + interval '1 month')
),
daily as (
  select
    dt,
    sum(amount) as daily_gmv
  from facts
  group by dt
),
running as (
  select
    dt,
    daily_gmv,
    sum(daily_gmv) over (
      order by dt
      rows between unbounded preceding and current row
    ) as running_gmv
  from daily
)
select
  dt,
  daily_gmv,
  running_gmv
from running
order by dt;
```

---

## 2) 7‑Day Moving Average (Daily Orders)
```sql
with
bounds as (
  select
    min(date_trunc('day', orders.created_at))::date as dmin,
    max(date_trunc('day', orders.created_at))::date as dmax
  from orders
),
days as (
  select generate_series(dmin, dmax, interval '1 day')::date as dt
  from bounds
),
order_counts as (
  select
    date_trunc('day', orders.created_at)::date as dt,
    count(orders.id) as order_cnt
  from orders
  group by date_trunc('day', orders.created_at)::date
),
counts as (
  select
    days.dt,
    coalesce(order_counts.order_cnt, 0) as order_cnt
  from days
  left join order_counts
    on order_counts.dt = days.dt
)
select
  dt,
  order_cnt,
  avg(order_cnt) over (
    order by dt
    rows between 3 preceding and 3 following
  ) as ma7
from counts
order by dt;
```

---

## 3) LAG & LEAD (Inter‑purchase Interval)
```sql
set search_path to app;

with
ordered as (
  select
    orders.user_id,
    orders.id as order_id,
    orders.created_at
  from orders
)
select
  user_id,
  order_id,
  created_at::date as order_dt,
  lag(created_at) over (
    partition by user_id
    order by created_at
  ) as prev_order_dt,
  extract(
    day from (created_at - lag(created_at) over (
      partition by user_id
      order by created_at
    ))
  ) as days_since_prev,
  lead(created_at) over (
    partition by user_id
    order by created_at
  ) as next_order_dt
from ordered
order by user_id, created_at;
```

---

## 4) INTERSECT & EXCEPT (Assortment Overlap)

**INTERSECT**
```sql
set search_path to app;

with
params as (
  select
    'Shop 10'::text as shop_a_name,
    'Shop 15'::text as shop_b_name
),
shop_a as (
  select shops.id as shop_id
  from shops
  inner join params
    on shops.name = params.shop_a_name
),
shop_b as (
  select shops.id as shop_id
  from shops
  inner join params
    on shops.name = params.shop_b_name
),
a as (
  select products.sku_id
  from products
  inner join shop_a
    on products.shop_id = shop_a.shop_id
  where products.is_active
),
b as (
  select products.sku_id
  from products
  inner join shop_b
    on products.shop_id = shop_b.shop_id
  where products.is_active
)
select sku_id from a
intersect
select sku_id from b;
```

**EXCEPT**
```sql
set search_path to app;

with
params as (
  select
    'Shop 10'::text as shop_a_name,
    'Shop 15'::text as shop_b_name
),
shop_a as (
  select shops.id as shop_id
  from shops
  inner join params
    on shops.name = params.shop_a_name
),
shop_b as (
  select shops.id as shop_id
  from shops
  inner join params
    on shops.name = params.shop_b_name
),
a as (
  select products.sku_id
  from products
  inner join shop_a
    on products.shop_id = shop_a.shop_id
  where products.is_active
),
b as (
  select products.sku_id
  from products
  inner join shop_b
    on products.shop_id = shop_b.shop_id
  where products.is_active
)
select sku_id from a
except
select sku_id from b;
```

---

## 5) Recursive CTE — Breadcrumbs
```sql
set search_path to app;

with recursive
anchor as (
  select
    categories.id,
    categories.parent_id,
    categories.name,
    categories.name::text as path
  from categories
  where categories.parent_id is null
),
recursive_member as (
  select
    anchor.id,
    anchor.parent_id,
    anchor.name,
    anchor.path
  from anchor
  union all
  select
    categories.id,
    categories.parent_id,
    categories.name,
    (recursive_member.path || ' > ' || categories.name)::text as path
  from categories
  inner join recursive_member
    on categories.parent_id = recursive_member.id
)
select
  id,
  parent_id,
  name,
  path
from recursive_member
order by path;
```

**Recursive CTE — GMV roll‑ups by leaf with root label**
```sql
with recursive
up as (
  select id, parent_id, name, id as root_id, name as root_name
  from categories parent_id is null
  union all
  select c.id, c.parent_id, c.name, up.root_id, up.root_name
  from categories
  inner join up up.id = c.parent_id
),
sales as (
  select
    oli.sku_id,
    sum(oli.line_total) as gmv
  from order_line_items
  inner join orders on o.id = oli.order_id
  where o.status in ('paid','shipped','delivered')
  group by oli.sku_id
),
sku_cat as (
  select s.id as sku_id, s.category_id
  from skus
),
leafs as (
  select c.id
  from categories
  left inner join categories on ch.parent_id = c.id
  where ch.id is null
)
select
  up.root_name as root_category,
  c.name       as leaf_category,
  coalesce(sum(sales.gmv), 0) as gmv
from leafs
inner join categories on c.id = l.id
inner join up up.id = c.id
left inner join sku_cat on sc.category_id = c.id
left inner join sales sales.sku_id = sc.sku_id
group by up.root_name, c.name
order by root_category, gmv desc;
```

---

## 6) FULL OUTER inner join — Catalog vs Sales Gaps
```sql
set search_path to app, public;

with
active_skus as (
  select distinct products.sku_id
  from products
  where products.is_active
),
sold_skus as (
  select distinct order_line_items.sku_id
  from order_line_items
  inner join orders
    on orders.id = order_line_items.order_id
  where orders.status in ('paid','shipped','delivered')
)
select
  coalesce(active_skus.sku_id, sold_skus.sku_id) as sku_id,
  (active_skus.sku_id is not null) as active_in_catalog,
  (sold_skus.sku_id is not null) as sold,
  case
    when active_skus.sku_id is not null and sold_skus.sku_id is not null then 'active & sold'
    when active_skus.sku_id is not null and sold_skus.sku_id is null     then 'active but no sales'
    when active_skus.sku_id is null     and sold_skus.sku_id is not null then 'sold but inactive now'
    else 'neither'
  end as notes
from active_skus
full outer join sold_skus
  on sold_skus.sku_id = active_skus.sku_id
order by sku_id;
```

---

## 7) Advanced LIKE / ILIKE / SIMILAR TO
**A. Titles starting with “Laptop ” (case‑insensitive)**
```sql
with
t as (
  select id, sku_code, title
  from app.skus
)
select id, sku_code, title
from t
where title ilike 'laptop%'
order by id

```

**B. Ends with '-13'**
```sql
with
t as (
  select id, sku_code
  from app.skus
)
select id, sku_code
from t
where sku_code like '%-13'
order by sku_code

```

**C. Includes anywhere '-13'**
```sql
with
t as (
  select id, sku_code
  from app.skus
)
select id, sku_code
from t
where sku_code like '%-13%'
order by sku_code

```

---

## 8) Cheapest Active Offer per SKU — **GROUP BY solution (no DISTINCT ON)**
```sql
with
offers as (
  select
    products.id as product_id,
    products.sku_id,
    products.price,
    products.shop_id,
    shops.rating
  from products
  inner join shops
    on shops.id = products.shop_id
  where products.is_active
),
sku_min_price as (
  select
    offers.sku_id,
    min(offers.price) as min_price
  from offers
  group by offers.sku_id
),
cheapest_offers as (
  select
    offers.sku_id,
    offers.product_id,
    offers.shop_id,
    offers.price,
    offers.rating
  from offers
  inner join sku_min_price
    on sku_min_price.sku_id = offers.sku_id
   and sku_min_price.min_price = offers.price
),
best_among_cheapest as (
  select
    cheapest_offers.sku_id,
    max(cheapest_offers.rating) as best_rating
  from cheapest_offers
  group by cheapest_offers.sku_id
),
final_choice as (
  select
    cheapest_offers.sku_id,
    min(cheapest_offers.product_id) as product_id,   -- deterministic tie-break
    min(cheapest_offers.shop_id)    as shop_id,
    min(cheapest_offers.price)      as price,        -- equals min_price
    max(cheapest_offers.rating)     as shop_rating   -- equals best_rating
  from cheapest_offers
  inner join best_among_cheapest
    on best_among_cheapest.sku_id = cheapest_offers.sku_id
   and best_among_cheapest.best_rating = cheapest_offers.rating
  group by cheapest_offers.sku_id
)
select *
from final_choice
order by sku_id;

```

---

## 9) GROUPING SETS — Day, Shop, Overall GMV
```sql
set search_path to app, public;
with
facts as (
  select
    date_trunc('day', orders.created_at)::date as dt,
    products.shop_id,
    order_line_items.line_total as amount
  from orders
  inner join order_line_items
    on order_line_items.order_id = orders.id
  inner join products
    on products.id = order_line_items.product_id
  where orders.status in ('paid','shipped','delivered')
)
select
  case
    when dt is not null and shop_id is null then 'by_day'
    when dt is null     and shop_id is not null then 'by_shop'
    when dt is null     and shop_id is null then 'overall'
    else 'by_day_shop'
  end as level,
  dt,
  shop_id,
  sum(amount) as gmv
from facts
group by grouping sets (
  (dt),
  (shop_id),
  ()
)
order by level, dt, shop_id

```

---

## 10) Conditional Aggregates with FILTER — Payment Outcomes
```sql
set search_path to app, public;
with
payments_daily as (
  select
    date_trunc('day', payments.created_at)::date as dt,
    payments.status,
    payments.amount
  from payments
)
select
  dt,
  sum(amount) filter (where status = 'captured')   as captured_amt,
  sum(amount) filter (where status = 'failed')     as failed_amt,
  sum(amount) filter (where status = 'refunded')   as refunded_amt
from payments_daily
group by dt
order by dt;
```
