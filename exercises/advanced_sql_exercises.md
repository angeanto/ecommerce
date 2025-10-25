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

---

## 2) Window Functions — 7‑Day Moving Average (Daily Orders)
**Goal:** Compute daily order counts and a **centered 7‑day moving average**.

**Expected columns:** `dt, order_cnt, ma7`

**Skeleton:**

---

## 3) Window Functions — LAG & LEAD (Inter‑purchase Interval)
**Goal:** For each user, compute previous order date, next order date, and days since previous order.

**Expected columns:** `user_id, order_id, order_dt, prev_order_dt, days_since_prev, next_order_dt`

**Skeleton:**

---

## 4) INTERSECT & EXCEPT — Overlapping Assortment
**Goal:** Compare SKUs offered by two shops: SKUs in common (INTERSECT) and SKUs unique to the first shop (EXCEPT).

**You must set the two shops in the `params` CTE.**

**Skeleton (INTERSECT):**

**Skeleton (EXCEPT):**

---

## 5) Recursive CTE — Category Breadcrumbs and Roll‑ups
**Goal A:** Produce full breadcrumb paths from root to every node.

**Expected columns:** `id, parent_id, name, path`

**Skeleton (Breadcrumbs):**

**Goal B:** GMV by leaf with root label.

**Expected columns:** `root_category, leaf_category, gmv`

**Skeleton (Roll‑ups):**

---

## 6) FULL OUTER JOIN — Catalog vs Sales Gap Analysis
**Goal:** Identify SKUs **active in catalog** but **no sales**, and SKUs **sold** but **inactive** now.

**Expected columns:** `sku_id, active_in_catalog, sold, notes`

**Skeleton:**

---

## 7) Advanced LIKE / ILIKE / SIMILAR TO / ESCAPE
**Goal A:** Case‑insensitive search — titles starting with “Laptop ”.

**Goal B:** Literal underscore in `sku_code`.

**Goal C:** `SIMILAR TO` for three prefixes.

---

## 8) Cheapest Active Offer per SKU — **Replace DISTINCT ON with GROUP BY**
**Goal:** For each SKU with active offers, return the **cheapest price** and the **best (highest) shop rating** among those cheapest offers, and then a **deterministic representative product**.

**Expected columns:** `sku_id, product_id, shop_id, price, shop_rating`

**Skeleton:**

---

## 9) GROUPING SETS — Multi‑level GMV (Day, Shop, Overall)
**Goal:** Produce a single result set with GMV by **day**, by **shop**, and the **overall total**.

**Expected columns:** `level, dt, shop_id, gmv`

**Skeleton:**

---

## 10) Conditional Aggregates with FILTER — Payment Outcomes
**Goal:** Daily amounts for each payment status.

**Expected columns:** `dt, captured_amt, authorized_amt, failed_amt, refunded_amt`

**Skeleton:**

