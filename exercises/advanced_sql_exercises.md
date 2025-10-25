# Advanced SQL Exercises (PostgreSQL) — E‑commerce Schema

These exercises are built for the Skroutz‑style marketplace schema you created earlier.  
**Conventions**  
- Use **CTEs** for every step (no nested subqueries).  
- Treat monetized orders as those with `orders.status IN ('paid','shipped','delivered')`.  
- Prefer `created_at::date` or `date_trunc('day', created_at)::date` when grouping by days.  
- When you need parameters, create a `params` CTE (e.g., pick specific shop ids).

---

## 1) Window Functions — Running Total (GMV)
**Goal:** Compute **daily GMV** and a **running total** for the current month.

**Expected columns:** `dt, daily_gmv, running_gmv`

**Skeleton:**
```sql
WITH
params AS (
  SELECT date_trunc('month', now())::date AS month_start
),
facts AS (
  SELECT
    date_trunc('day', o.created_at)::date AS dt,
    oli.line_total AS amount
  FROM orders o
  JOIN order_line_items oli ON oli.order_id = o.id
  WHERE o.status IN ('paid','shipped','delivered')
    AND o.created_at >= (SELECT month_start FROM params)
    AND o.created_at <  ((SELECT month_start FROM params) + interval '1 month')
),
daily AS (
  SELECT dt, SUM(amount) AS daily_gmv
  FROM facts
  GROUP BY dt
)
SELECT
  dt,
  daily_gmv,
  /* running total here */
  SUM(daily_gmv) OVER (
    ORDER BY dt
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_gmv
FROM daily
ORDER BY dt;
```

---

## 2) Window Functions — 7‑Day Moving Average (Daily Orders)
**Goal:** Compute daily order counts and a **centered 7‑day moving average**.

**Expected columns:** `dt, order_cnt, ma7`

**Skeleton:**
```sql
WITH
bounds AS (
  SELECT
    MIN(date_trunc('day', created_at))::date AS dmin,
    MAX(date_trunc('day', created_at))::date AS dmax
  FROM orders
),
days AS (
  SELECT generate_series(dmin, dmax, interval '1 day')::date AS dt
  FROM bounds
),
order_counts AS (
  SELECT date_trunc('day', created_at)::date AS dt, COUNT(*) AS order_cnt
  FROM orders
  GROUP BY date_trunc('day', created_at)::date
),
counts AS (
  SELECT d.dt, COALESCE(oc.order_cnt, 0) AS order_cnt
  FROM days d
  LEFT JOIN order_counts oc ON oc.dt = d.dt
)
SELECT
  dt,
  order_cnt,
  AVG(order_cnt) OVER (
    ORDER BY dt
    ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
  ) AS ma7
FROM counts
ORDER BY dt;
```

---

## 3) Window Functions — LAG & LEAD (Inter‑purchase Interval)
**Goal:** For each user, compute previous order date, next order date, and days since previous order.

**Expected columns:** `user_id, order_id, order_dt, prev_order_dt, days_since_prev, next_order_dt`

**Skeleton:**
```sql
WITH
ordered AS (
  SELECT
    o.user_id,
    o.id AS order_id,
    o.created_at
  FROM orders o
)
SELECT
  user_id,
  order_id,
  created_at::date AS order_dt,
  LAG(created_at)  OVER (PARTITION BY user_id ORDER BY created_at) AS prev_order_dt,
  EXTRACT(DAY FROM (created_at - LAG(created_at) OVER (PARTITION BY user_id ORDER BY created_at))) AS days_since_prev,
  LEAD(created_at) OVER (PARTITION BY user_id ORDER BY created_at) AS next_order_dt
FROM ordered
ORDER BY user_id, created_at;
```

---

## 4) INTERSECT & EXCEPT — Overlapping Assortment
**Goal:** Compare SKUs offered by two shops: SKUs in common (INTERSECT) and SKUs unique to the first shop (EXCEPT).

**You must set the two shops in the `params` CTE.**

**Skeleton (INTERSECT):**
```sql
WITH
params AS (
  SELECT 1::bigint AS shop_a, 2::bigint AS shop_b  -- set actual ids
),
a AS (
  SELECT sku_id FROM products WHERE is_active AND shop_id = (SELECT shop_a FROM params)
),
b AS (
  SELECT sku_id FROM products WHERE is_active AND shop_id = (SELECT shop_b FROM params)
)
SELECT sku_id FROM a
INTERSECT
SELECT sku_id FROM b;
```

**Skeleton (EXCEPT):**
```sql
WITH
params AS (
  SELECT 1::bigint AS shop_a, 2::bigint AS shop_b  -- set actual ids
),
a AS (
  SELECT sku_id FROM products WHERE is_active AND shop_id = (SELECT shop_a FROM params)
),
b AS (
  SELECT sku_id FROM products WHERE is_active AND shop_id = (SELECT shop_b FROM params)
)
SELECT sku_id FROM a
EXCEPT
SELECT sku_id FROM b;
```

---

## 5) Recursive CTE — Category Breadcrumbs and Roll‑ups
**Goal A:** Produce full breadcrumb paths from root to every node.

**Expected columns:** `id, parent_id, name, path`

**Skeleton (Breadcrumbs):**
```sql
WITH RECURSIVE
roots AS (
  SELECT id, parent_id, name, name::text AS path
  FROM categories
  WHERE parent_id IS NULL
),
walk AS (
  SELECT * FROM roots
  UNION ALL
  SELECT
    c.id, c.parent_id, c.name,
    (w.path || ' > ' || c.name)::text AS path
  FROM categories c
  JOIN walk w ON w.id = c.parent_id
)
SELECT id, parent_id, name, path
FROM walk
ORDER BY path;
```

**Goal B:** GMV by leaf with root label.

**Expected columns:** `root_category, leaf_category, gmv`

**Skeleton (Roll‑ups):**
```sql
WITH RECURSIVE
up AS (
  SELECT id, parent_id, name, id AS root_id, name AS root_name
  FROM categories
  WHERE parent_id IS NULL
  UNION ALL
  SELECT c.id, c.parent_id, c.name, up.root_id, up.root_name
  FROM categories c
  JOIN up ON up.id = c.parent_id
),
sales AS (
  SELECT
    oli.sku_id,
    SUM(oli.line_total) AS gmv
  FROM order_line_items oli
  JOIN orders o ON o.id = oli.order_id
  WHERE o.status IN ('paid','shipped','delivered')
  GROUP BY oli.sku_id
),
sku_cat AS (
  SELECT s.id AS sku_id, s.category_id
  FROM skus s
),
leafs AS (
  SELECT c.id
  FROM categories c
  LEFT JOIN categories ch ON ch.parent_id = c.id
  WHERE ch.id IS NULL
)
SELECT
  up.root_name AS root_category,
  c.name       AS leaf_category,
  COALESCE(SUM(sales.gmv), 0) AS gmv
FROM leafs l
JOIN categories c ON c.id = l.id
JOIN up ON up.id = c.id
LEFT JOIN sku_cat sc ON sc.category_id = c.id
LEFT JOIN sales ON sales.sku_id = sc.sku_id
GROUP BY up.root_name, c.name
ORDER BY root_category, gmv DESC;
```

---

## 6) FULL OUTER JOIN — Catalog vs Sales Gap Analysis
**Goal:** Identify SKUs **active in catalog** but **no sales**, and SKUs **sold** but **inactive** now.

**Expected columns:** `sku_id, active_in_catalog, sold, notes`

**Skeleton:**
```sql
WITH
active_skus AS (
  SELECT DISTINCT sku_id FROM products WHERE is_active
),
sold_skus AS (
  SELECT DISTINCT oli.sku_id
  FROM order_line_items oli
  JOIN orders o ON o.id = oli.order_id
  WHERE o.status IN ('paid','shipped','delivered')
)
SELECT
  COALESCE(a.sku_id, s.sku_id) AS sku_id,
  (a.sku_id IS NOT NULL) AS active_in_catalog,
  (s.sku_id IS NOT NULL) AS sold,
  CASE
    WHEN a.sku_id IS NOT NULL AND s.sku_id IS NOT NULL THEN 'active & sold'
    WHEN a.sku_id IS NOT NULL AND s.sku_id IS NULL  THEN 'active but no sales'
    WHEN a.sku_id IS NULL  AND s.sku_id IS NOT NULL THEN 'sold but inactive now'
    ELSE 'neither'
  END AS notes
FROM active_skus a
FULL OUTER JOIN sold_skus s ON s.sku_id = a.sku_id
ORDER BY sku_id;
```

---

## 7) Advanced LIKE / ILIKE / SIMILAR TO / ESCAPE
**Goal A:** Case‑insensitive search — titles starting with “Laptop ”.

```sql
WITH
t AS (
  SELECT id, sku_code, title FROM skus
)
SELECT id, sku_code, title
FROM t
WHERE title ILIKE 'Laptop %'
ORDER BY id
LIMIT 20;
```

**Goal B:** Literal underscore in `sku_code`.

```sql
WITH
t AS (
  SELECT id, sku_code FROM skus
)
SELECT id, sku_code
FROM t
WHERE sku_code LIKE '%SKU-1\_%' ESCAPE '\';
```

**Goal C:** `SIMILAR TO` for three prefixes.

```sql
WITH
t AS (
  SELECT id, title FROM skus
)
SELECT id, title
FROM t
WHERE title SIMILAR TO '(Smartphone|Laptop|Blender)%'
ORDER BY id
LIMIT 20;
```

---

## 8) Cheapest Active Offer per SKU — **Replace DISTINCT ON with GROUP BY**
**Goal:** For each SKU with active offers, return the **cheapest price** and the **best (highest) shop rating** among those cheapest offers, and then a **deterministic representative product**.

**Expected columns:** `sku_id, product_id, shop_id, price, shop_rating`

**Skeleton:**
```sql
WITH
offers AS (
  SELECT p.id AS product_id, p.sku_id, p.price, p.shop_id, s.rating
  FROM products p
  JOIN shops s ON s.id = p.shop_id
  WHERE p.is_active
),
sku_min_price AS (
  SELECT sku_id, MIN(price) AS min_price
  FROM offers
  GROUP BY sku_id
),
cheapest_offers AS (
  SELECT o.sku_id, o.product_id, o.shop_id, o.price, o.rating
  FROM offers o
  JOIN sku_min_price m
    ON m.sku_id = o.sku_id AND m.min_price = o.price
),
best_among_cheapest AS (
  SELECT sku_id, MAX(rating) AS best_rating
  FROM cheapest_offers
  GROUP BY sku_id
),
final_choice AS (
  SELECT
    c.sku_id,
    MIN(c.product_id) AS product_id,   -- deterministic pick when ties remain
    MIN(c.shop_id)    AS shop_id,
    MIN(c.price)      AS price,        -- all equal to min_price anyway
    MAX(c.rating)     AS shop_rating   -- equals best_rating
  FROM cheapest_offers c
  JOIN best_among_cheapest b
    ON b.sku_id = c.sku_id AND b.best_rating = c.rating
  GROUP BY c.sku_id
)
SELECT *
FROM final_choice
ORDER BY sku_id;
```

---

## 9) GROUPING SETS — Multi‑level GMV (Day, Shop, Overall)
**Goal:** Produce a single result set with GMV by **day**, by **shop**, and the **overall total**.

**Expected columns:** `level, dt, shop_id, gmv`

**Skeleton:**
```sql
WITH
facts AS (
  SELECT
    date_trunc('day', o.created_at)::date AS dt,
    p.shop_id,
    oli.line_total AS amount
  FROM orders o
  JOIN order_line_items oli ON oli.order_id = o.id
  JOIN products p ON p.id = oli.product_id
  WHERE o.status IN ('paid','shipped','delivered')
)
SELECT
  CASE
    WHEN dt IS NOT NULL AND shop_id IS NULL THEN 'by_day'
    WHEN dt IS NULL     AND shop_id IS NOT NULL THEN 'by_shop'
    WHEN dt IS NULL     AND shop_id IS NULL THEN 'overall'
    ELSE 'by_day_shop'
  END AS level,
  dt,
  shop_id,
  SUM(amount) AS gmv
FROM facts
GROUP BY GROUPING SETS (
  (dt),
  (shop_id),
  ()
)
ORDER BY level, dt NULLS LAST, shop_id NULLS LAST;
```

---

## 10) Conditional Aggregates with FILTER — Payment Outcomes
**Goal:** Daily amounts for each payment status.

**Expected columns:** `dt, captured_amt, authorized_amt, failed_amt, refunded_amt`

**Skeleton:**
```sql
WITH
payments_daily AS (
  SELECT
    date_trunc('day', created_at)::date AS dt,
    status,
    amount
  FROM payments
)
SELECT
  dt,
  SUM(amount) FILTER (WHERE status = 'captured')   AS captured_amt,
  SUM(amount) FILTER (WHERE status = 'authorized') AS authorized_amt,
  SUM(amount) FILTER (WHERE status = 'failed')     AS failed_amt,
  SUM(amount) FILTER (WHERE status = 'refunded')   AS refunded_amt
FROM payments_daily
GROUP BY dt
ORDER BY dt;
```
