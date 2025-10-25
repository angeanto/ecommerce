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
    date_trunc('day', o.created_at)::date as dt,
    oli.line_total as amount
  from orders
  inner join order_line_items on oli.order_id = o.id
  where o.status in ('paid','shipped','delivered')
    and o.created_at >= (select month_start from params)
    and o.created_at <  ((select month_start from params) + interval '1 month')
),
daily as (
  select dt, sum(amount) as daily_gmv
  from facts by dt
)
select
  dt,
  daily_gmv,
  sum(daily_gmv) over (
    order by dt
    rows between unbounded preceding and current row
  ) as running_gmv
from daily by dt;
```

---

## 2) 7‑Day Moving Average (Daily Orders)
```sql
with
bounds as (
  select
    min(date_trunc('day', created_at))::date as dmin,
    max(date_trunc('day', created_at))::date as dmax
  from orders
),
days as (
  select generate_series(dmin, dmax, interval '1 day')::date as dt
  from bounds
),
order_counts as (
  select date_trunc('day', created_at)::date as dt, count(*) as order_cnt
  from orders by date_trunc('day', created_at)::date
),
counts as (
  select d.dt, coalesce(oc.order_cnt, 0) as order_cnt
  from days
  left inner join order_counts on oc.dt = d.dt
)
select
  dt,
  order_cnt,
  avg(order_cnt) over (
    order by dt
    rows between 3 preceding and 3 following
  ) as ma7
from counts by dt;
```

---

## 3) LAG & LEAD (Inter‑purchase Interval)
```sql
with
ordered as (
  select
    o.user_id,
    o.id as order_id,
    o.created_at
  from orders
)
select
  user_id,
  order_id,
  created_at::date as order_dt,
  lag(created_at)  over (partition by user_id order by created_at) as prev_order_dt,
  extract(day from (created_at - lag(created_at) over (partition by user_id order by created_at))) as days_since_prev,
  lead(created_at) over (partition by user_id order by created_at) as next_order_dt
from ordered by user_id, created_at;
```

---

## 4) INTERSECT & EXCEPT (Assortment Overlap)
**Pick two shops deterministically so the query always runs.**
```sql
with
shop_list as (
  select id, row_number() over (order by id) as rn
  from shops
),
params as (
  select
    max(case when rn = 1 then id end)::bigint as shop_a,
    max(case when rn = 2 then id end)::bigint as shop_b
  from shop_list
),
a as (
  select sku_id
  from products is_active and shop_id = (select shop_a from params)
),
b as (
  select sku_id
  from products is_active and shop_id = (select shop_b from params)
)
-- intersect
select sku_id from a
select sku_id from b;
```

**EXCEPT variant:**
```sql
with
shop_list as (
  select id, row_number() over (order by id) as rn
  from shops
),
params as (
  select
    max(case when rn = 1 then id end)::bigint as shop_a,
    max(case when rn = 2 then id end)::bigint as shop_b
  from shop_list
),
a as (
  select sku_id
  from products is_active and shop_id = (select shop_a from params)
),
b as (
  select sku_id
  from products is_active and shop_id = (select shop_b from params)
)
select sku_id from a
select sku_id from b;
```

---

## 5) Recursive CTE — Breadcrumbs
```sql
with recursive
roots as (
  select id, parent_id, name, name::text as path
  from categories parent_id is null
),
walk as (
  select * from roots all
  select
    c.id, c.parent_id, c.name,
    (w.path || ' > ' || c.name)::text as path
  from categories
  inner join walk on w.id = c.parent_id
)
select id, parent_id, name, path
from walk by path;
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
with
active_skus as (
  select distinct sku_id from products is_active
),
sold_skus as (
  select distinct oli.sku_id
  from order_line_items
  inner join orders on o.id = oli.order_id
  where o.status in ('paid','shipped','delivered')
)
select
  coalesce(a.sku_id, s.sku_id) as sku_id,
  (a.sku_id is not null) as active_in_catalog,
  (s.sku_id is not null) as sold,
  case
    when a.sku_id is not null and s.sku_id is not null then 'active & sold'
    when a.sku_id is not null and s.sku_id is null  then 'active but no sales'
    when a.sku_id is null  and s.sku_id is not null then 'sold but inactive now'
    else 'neither'
  end as notes
from active_skus
full outer inner join sold_skus on s.sku_id = a.sku_id
order by sku_id;
```

---

## 7) Advanced LIKE / ILIKE / SIMILAR TO
**A. Titles starting with “Laptop ” (case‑insensitive)**
```sql
with
t as (
  select id, sku_code, title from skus
)
select id, sku_code, title
from t title ilike 'laptop %'
order by id
limit 20;
```

**B. Literal underscore in `sku_code`**
```sql
with
t as (
  select id, sku_code from skus
)
select id, sku_code
from t sku_code like '%sku-1\_%' escape '\';
```

**C. `SIMILAR TO` three prefixes**
```sql
with
t as (
  select id, title from skus
)
select id, title
from t title similar to '(smartphone|laptop|blender)%'
order by id
limit 20;
```

---

## 8) Cheapest Active Offer per SKU — **GROUP BY solution (no DISTINCT ON)**
```sql
with
offers as (
  select p.id as product_id, p.sku_id, p.price, p.shop_id, s.rating
  from products
  inner join shops on s.id = p.shop_id
  where p.is_active
),
sku_min_price as (
  select sku_id, min(price) as min_price
  from offers by sku_id
),
cheapest_offers as (
  select o.sku_id, o.product_id, o.shop_id, o.price, o.rating
  from offers
  inner join sku_min_price
    on m.sku_id = o.sku_id and m.min_price = o.price
),
best_among_cheapest as (
  select sku_id, max(rating) as best_rating
  from cheapest_offers by sku_id
),
final_choice as (
  select
    c.sku_id,
    min(c.product_id) as product_id,   -- deterministic choice among ties
    min(c.shop_id)    as shop_id,
    min(c.price)      as price,        -- equals min_price
    max(c.rating)     as shop_rating   -- equals best_rating
  from cheapest_offers
  inner join best_among_cheapest
    on b.sku_id = c.sku_id and b.best_rating = c.rating
  group by c.sku_id
)
select *
from final_choice by sku_id;
```

---

## 9) GROUPING SETS — Day, Shop, Overall GMV
```sql
with
facts as (
  select
    date_trunc('day', o.created_at)::date as dt,
    p.shop_id,
    oli.line_total as amount
  from orders
  inner join order_line_items on oli.order_id = o.id
  inner join products on p.id = oli.product_id
  where o.status in ('paid','shipped','delivered')
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
from facts by grouping sets (
  (dt),
  (shop_id),
  ()
)
order by level, dt nulls last, shop_id nulls last;
```

---

## 10) Conditional Aggregates with FILTER — Payment Outcomes
```sql
with
payments_daily as (
  select
    date_trunc('day', created_at)::date as dt,
    status,
    amount
  from payments
)
select
  dt,
  sum(amount) filter (where status = 'captured')   as captured_amt,
  sum(amount) filter (where status = 'authorized') as authorized_amt,
  sum(amount) filter (where status = 'failed')     as failed_amt,
  sum(amount) filter (where status = 'refunded')   as refunded_amt
from payments_daily by dt
order by dt;
```
