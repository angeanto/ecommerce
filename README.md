# 📘 E-Commerce Analytics Database — Amazon-Style Marketplace Schema

## Overview

This repository provides a **complete PostgreSQL database schema and data generation framework** inspired by the **Amazon e-commerce ecosystem**.  
It is designed as a **realistic, educational, and analytical environment** for demonstrating **advanced SQL concepts, relational modeling, and business analytics**.

The database models the essential structure of a modern marketplace where multiple shops can sell the same SKUs, customers place orders, products belong to hierarchical categories, and payments are tracked with real-world business rules.  
Synthetic yet logically consistent data enables deep analytical exploration across marketing, sales, and customer domains.

---

## 🎯 Project Objectives

This repository serves as both a **teaching platform** and a **reference data model** for:

1. **Data modeling excellence** — normalization, referential integrity, and business constraints.
2. **Comprehensive SQL practice** — from simple selections to recursive and windowed queries.
3. **E-commerce analytics** — revenue, performance, product overlap, payment outcomes, etc.
4. **Realistic business logic** — lifecycle consistency across users, orders, payments, and shops.
5. **Data storytelling** — bridging raw transactional data and executive-level insights.

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

## 🧾 License & Usage

Licensed under the **MIT License**.

You may freely use this repository for **educational**, **demonstration**, or **instructional** purposes — in universities, online courses, or professional workshops.

Attribution is appreciated:
> “Based on the Amazon-Style E-Commerce Analytics Database by [Your Name / Organization]”

---

## 👨‍🏫 Author

This project is created by a **Antonis Angelakis**,  
focused on building **realistic, hands-on data environments** that bridge theory and business analytics practice. 

It forms part of a broader initiative to teach **mindful, applied data analytics** — connecting data, logic, and business context.
