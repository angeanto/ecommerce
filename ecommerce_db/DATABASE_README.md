# 📘 E-Commerce Analytics Database — Amazon-Style Marketplace Schema

## Overview
<img width="1797" height="790" alt="tech_stack drawio" src="https://github.com/user-attachments/assets/afd56add-ac2e-46bf-9fbe-c214aaacd135" />

This repository provides a **complete PostgreSQL database schema and data generation framework** inspired by the **Amazon e-commerce ecosystem**.
It is designed as a **realistic, educational, and analytical environment** for demonstrating **advanced SQL concepts, relational modeling, and business analytics**.

The database models the essential structure of a modern marketplace where multiple shops can sell the same SKUs, customers place orders, products belong to hierarchical categories, and payments are tracked with real-world business rules.
Synthetic yet logically consistent data enables deep analytical exploration across marketing, sales, and customer domains.

The repo also contains a production-grade dbt project built on top of an Amazon-style e-commerce dataset.
The project demonstrates real-world analytics engineering, including:

- Medallion modeling (staging → intermediate → reporting)
- Dimensional modeling (SCD Type 2)
- Date dimensions & reporting periods
- Window functions, recursive CTEs, and advanced SQL logic
- Fully documented models and a comprehensive test suite
- Business-meaningful KPIs and semantic models

This serves as an end-to-end example of how to structure, document, test and operate a dbt project at a professional level.

ecommerce_db
│ ── models/
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│ ── staging/           → Raw → cleaned sources
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│ ── intermediate/      → Business logic, metrics, calculations
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│ ── reporting/         → Analytics-ready tables, KPIs, dimensional joins
│&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;│ ── current_views/     → SCD Type 2 "current" tables (dimension versions)
│ ── snapshots/             → SCD Type 2 snapshots
│ ── macros/                → UDFs, utilities, reusable logic
│ ── tests/                 → Custom schema tests
│ ── seeds/                 → Seed data (e.g., Greek holidays)

This mirrors real production environments where:
- Staging standardizes raw input
- Intermediate expresses business rules
- Reporting exposes metrics to BI & ML
- Current Views deliver SCD2 “current rows”
- Snapshots hold historical dimension data
---

## 🎯 Project Objectives

This repository serves as both a **teaching platform** and a **reference data model** for:

1. **Data modeling excellence** —> normalization, referential integrity, and business constraints.
2. **Comprehensive SQL practice** —> from simple selections to recursive and windowed queries.
3. **E-commerce analytics** —> revenue, performance, product overlap, payment outcomes, etc.
4. **Realistic business logic** —> lifecycle consistency across users, orders, payments, and shops.
5. **Data storytelling** —> bridging raw transactional data and executive-level insights.

---

## 🧱 Schema Overview

All entities reside under the **`app` schema** and are designed for **PostgreSQL 12+**.

### Core Tables

| Table | Description |
|--------|-------------|
| **users** | Registered customers, each with unique identifiers and activity status. |
| **addresses** | Physical addresses, optionally linked to multiple users. |
| **users_addresses** | Bridge table for many-to-many user–address relationships, with shipping/billing flags. |
| **categories** | Hierarchical taxonomy of product categories using self-referencing keys. |
| **skus** | Abstract catalog items representing a unique product identity (brand + attributes). |
| **shops** | Independent merchants selling SKUs within the marketplace. |
| **products** | Shop-specific offers for SKUs with prices, stock, and active status. |
| **orders** | Customer purchase orders containing order status, payment link, and timestamps. |
| **order_line_items** | Line-level detail linking each order to SKUs and products. |
| **payments** | Payment transactions for orders, tracking provider, status, and amounts. |

The schema ensures **referential integrity**, **normalized relationships**, and **consistent data semantics**.
Foreign keys, constraints, and enumerations maintain integrity and realism throughout.

---

## 🧮 Data Generation

Synthetic data scripts populate the system with thousands of records that simulate real operations:

- 1,200 users and corresponding addresses
- 600 SKUs distributed across 20+ product categories
- 50 shops with varying activity and rating levels
- Thousands of orders and payments reflecting diverse outcomes (`paid`, `authorized`, `failed`, `refunded`)

Additionally, specialized demo records are created to support analytical cases:

- **`SKU-ACTIVE-SOLD`** → product currently active and with successful sales
- **`SKU-SOLD-INACTIVE`** → product previously sold but now inactive

These examples are critical for exploring realistic full outer joins, category hierarchies, and payment performance scenarios.

---

## 🧠 Advanced SQL Exercises

This repository includes two complementary learning assets:

- `advanced_sql_exercises.md` — problem set with task descriptions only
- `advanced_sql_answers.md` — complete, validated solutions

Each exercise is written with:
- **Lowercase SQL keywords**
- **Explicit join types**
- **CTEs only (no nested subqueries)**
- **Consistent use of the `app` schema**
- **Realistic business context**

### Key Topics

| SQL Feature | Example Scenario |
|--------------|------------------|
| **Window Functions (Running Totals, LAG/LEAD)** | Customer order history and daily GMV tracking |
| **Recursive CTEs** | Expanding category hierarchies into full breadcrumb paths |
| **GROUPING SETS** | Computing GMV by day, by shop, and overall totals in one query |
| **FILTER Clauses** | Splitting payment outcomes into distinct aggregates |
| **Pattern Matching (LIKE / ILIKE / SIMILAR TO)** | Searching SKUs or titles using advanced string operators |
| **Full Outer Joins** | Comparing active vs. sold SKUs for business coverage |
| **Deterministic Aggregation (GROUP BY)** | Selecting cheapest active offers per SKU |
| **COALESCE & CASE Logic** | Building business labels and reconciliation reports |

These exercises go beyond syntax to teach **how analytical SQL expresses business understanding**.

---

## 🧩 Learning and Educational Use

This project is ideal for:

- **University courses** in data analytics and data engineering
- **Corporate data literacy and SQL upskilling programs**
- **Analyst onboarding labs** for e-commerce data pipelines
- **Independent study and portfolio development**

It provides a **holistic and story-driven environment** for practicing real-world analytical SQL and understanding how data supports business decisions.

---

## 📊 Example Analytical Queries

- **Top categories by sales revenue**
  ```sql
  select categories.name, sum(order_line_items.line_total) as gmv
  from app.order_line_items
  inner join app.skus
    on skus.id = order_line_items.sku_id
  inner join app.categories
    on categories.id = skus.category_id
  group by categories.name
  order by gmv desc
  limit 5;
  ```

- **Monthly revenue growth**
  ```sql
  select
    date_trunc('month', created_at)::date as month,
    sum(total_amount) as revenue
  from app.orders
  where status in ('paid','shipped','delivered')
  group by month
  order by month;
  ```

- **Shop performance**
  ```sql
  select shops.name, count(distinct orders.id) as orders, sum(orders.total_amount) as gmv
  from app.orders
  inner join app.order_line_items
    on order_line_items.order_id = orders.id
  inner join app.products
    on products.id = order_line_items.product_id
  inner join app.shops
    on shops.id = products.shop_id
  where orders.status in ('paid','shipped','delivered')
  group by shops.name
  order by gmv desc;
  ```

---

## 🧰 Technical Highlights

- **Enum domains** for key business states (`order_status`, `payment_status`, etc.)
- **Transactional DDL/DML** for integrity and rollback safety
- **Derived columns** for accurate totals (`line_total = unit_price * quantity`)
- **Self-referential recursion** for hierarchical categories
- **Indexes and constraints** tuned for analytical workloads
- **Data consistency** between orders, payments, and products

---

🧱 Modeling Layers (Medallion)
1️⃣ Staging Layer (stg_)

Purpose: clean, rename, cast, and standardize raw tables.

Key characteristics:

- One model per raw table
- Consistent naming (id, created_at, status)
- No business logic — only cleaning
- Includes new model: stg_all_dates
- Primary keys are tested with:
- not_null
- unique
- Composite PKs use

`- dbt_utils.unique_combination_of_columns:
    combination_of_columns: [user_id, address_id]
`

2️⃣ Intermediate Layer (int_)

Purpose: express calculations and business metrics.

Contains logic for business entities such us Revenue per day, shop, categorym, User behavior (orders, revenue, first/last order), Category revenue etc

No dimensional enrichments here. Only business logic.


3️⃣ Reporting Layer (rep_)

Purpose: analytics-ready, dimensional, enriched models.

Examples:`rep_main_kpis_per_shop, rep_revenue_per_period → aggregated Day / Month / Year, rep_user_kpis and rep_user_kpis_per_period
, rep_category_performance enriched by category dimensions, rep_payment_summary, rep_product_performance` etc

All reporting models include:

- Calendar & date dimension joins

- Lag/lead for previous/next period

- Rolling sums

- Basket of qualitative enrichments (weekday, holiday, seasonality)

This layer is intended for BI dashboards, ML features, and executive reporting.

4️⃣ Current Views (dim_*_current)

These are SCD Type 2 “current row only” views built on top of snapshots.

Example: **dim_addresses_current based** on snapshot addresses_hist

📅 Date Dimension & Reporting Periods

This project includes an industrial-grade calendar system: **stg_all_dates**

Generates a continuous date spine (2015 — Present).

dim_reporting_periods

Produces:

- Day

- Week

- Month

- Quarter

- Year

For each reporting grain, including:

Weekend flags

- Greek public holidays (via seeds)

- Day name, month name, ISO week

- Seasonal labels

Used by reporting models such as:

- rep_revenue_per_period

- rep_user_kpis_per_period

🧪 Testing Strategy

Each layer includes targeted tests.

Primary Key Tests

- unique

- not_null

- unique_combination_of_columns for composite PKs

Examples:
```
tests:
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns:
        - user_id
        - reporting_period
        - reporting_date
```
Referential Integrity Tests
Where appropriate:
```
- relationships:
    field: user_id
    to: ref('stg_users')
    column: user_id
```
Data Quality Tests
- Accepted values (status, enums)
- Freshness (on sources)
- Row-level constraints
📄 Documentation Strategy
Generate column+model YAML automatically

Requires package:

```
packages:
  - package: dbt-labs/dbt_codegen
    version: ">=0.12.1"
```


Generate:

```
dbt run-operation generate_model_yaml --args '{"model_name": "stg_orders"}'
```

Generate for multiple models:

```
dbt run-operation generate_model_yaml --args '{
  "model_names": ["int_revenue_daily", "int_user_orders"]
}'
```

Auto-discover models from a folder:

```
dbt ls -m models/intermediate --output name \
  | xargs -I{} echo -n '"{}",'
```

Then wrap inside:
```
dbt run-operation generate_model_yaml --args '{"model_names":[ ... ]}'
```
⚙️ Essential dbt Commands (Complete 360° List)
▶ Build everything
```
dbt build
```
▶ Run models only
```
dbt run
```

Run a specific folder:
```
dbt run --select staging
dbt run --select models/intermediate
dbt run --select tag:finance
```

Specific model:
```
dbt run --select rep_revenue_per_period
```
▶ Test everything
```
dbt test
```

Test only staging:
```
dbt test --select staging
```

Test one model:
```
dbt test --select stg_orders
```

Test only PK tests:
```
dbt test --select test_type:unique
dbt test --select test_type:not_null
```
▶ Generate docs
```
dbt docs generate
```

Serve documentation locally:
```
dbt docs serve
```
▶ Snapshots (SCD2)

Run snapshots:
```
dbt snapshot
```
▶ Seed files (e.g., Greek Holidays)
```
dbt seed
```
▶ Build only downstream dependencies
```
dbt build --select +int_user_orders
```

Build entire graph around a model:
```
dbt build --select int_user_orders+
```
▶ Debug environment
```
dbt debug
```
🧩 Business Logic Highlights
- Window Functions
- Used across intermediate & reporting models:
- Previous/next period revenue
- Rolling sums (7-day, 30-day, 7-month)
- Running totals
- Window averages
- Recursive CTEs
- int_category_hierarchy builds a depth-first tree of category → parent → root.
- SCD Type 2
- Address dimension tracked historically with snapshot:
- addresses_hist
- dim_addresses_current
- High-value KPI reporting
- Lifetime revenue per user
- Shop-level GMV
- Category performance
- Time-grain revenue modeling
- Payment provider funnel performance

## 🧾 License & Usage
Licensed under the **MIT License**.
---

🎓 Learning Value (for mentees & teams)

This repository demonstrates:
✔ Proper medallion architecture
✔ Business-aligned dimensional modeling
✔ SCD2 implementation
✔ Calendar table engineering
✔ Clean SQL conventions (leading commas, CTEs, modularity)
✔ dbt testing & documentation best practices
✔ Automated YAML generation
✔ Clear separation of logic & semantics

Ideal for:
- Data analytics students
- Junior data engineers / analytics engineers
- BI teams migrating to dbt
- Portfolio demonstration projects

🙌 Author

Antonis Angelakis, Principal BI Consultant & Instructor, DataConscious – A mindful approach to analytics
