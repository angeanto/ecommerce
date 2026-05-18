# E-Commerce DB Case Study

This folder contains the technical case study for Data Career Accelerator. It includes the raw PostgreSQL schema and seed data, advanced SQL exercises, installation notes, and a dbt project that transforms the marketplace data into analytics-ready models.

## What This Database Demonstrates

- Operational e-commerce entities: users, addresses, shops, products, SKUs, categories, orders, order line items, and payments.
- Analytical modeling patterns: staging, intermediate business logic, reporting marts, and current views.
- dbt practices: sources, refs, tests, documentation, snapshots, and model layering.
- Analytics concepts used across the course: grain, joins, KPI definitions, payment outcomes, shop performance, category revenue, governance, and portfolio proof.

## Folder Map

```text
ecommerce_db/
|-- create-insert-data/       # PostgreSQL schema and seed data
|-- exercises/                # SQL practice material
|-- installations_etc/        # Local setup and integration notes
|-- models/                   # dbt models
|-- snapshots/                # dbt snapshots
|-- macros/                   # dbt macros
|-- DATABASE_README.md        # Original detailed database reference
|-- dbt_project.yml
`-- packages.yml
```

## Raw Database Setup

From this folder, run the schema and seed script against a local PostgreSQL database:

```bash
psql -d <database_name> -f create-insert-data/ecommerce_create_insert.sql
```

The script creates the `app` schema and sample marketplace data.

## dbt Workflow

```bash
cd ecommerce_db
dbt deps
dbt debug
dbt run --select staging
dbt run --select intermediate
dbt run --select reporting
dbt test
dbt docs generate
dbt docs serve
```

## Course Usage

- Session 2 uses the raw schema to teach SQL, grain, facts, dimensions, and modeling foundations.
- Session 3 uses the dbt project to teach pipelines, tests, documentation, and Git workflow.
- Session 4 uses reporting models for dashboard and storytelling examples.
- Session 5 uses shop, payment, and category metrics for KPI trees and trade-off analysis.
- Session 6 uses model tests, definitions, and ownership as governance examples.
- Session 7 uses the schema as context for AI prompting and verification.
- Session 8 uses the project as portfolio proof for CV, LinkedIn, and interview stories.
