# Examples - Session 2

These examples use the raw Postgres schema. Run `set search_path to app;` first.

## Example 1 - Identify the grain

`orders` has one row per order. `order_line_items` has one row per product line inside an order. Joining them changes the grain.

```sql
set search_path to app;

select
    o.id as order_id,
    count(oli.id) as line_count,
    sum(oli.line_total) as line_total_sum,
    max(o.total_amount) as order_total
from orders o
join order_line_items oli
    on oli.order_id = o.id
group by 1
order by line_count desc
limit 20;
```

## Example 2 - Build an analytical query from facts and dimensions

```sql
with monetized_lines as (
    select
        o.created_at::date as order_date,
        s.name as shop_name,
        c.name as category_name,
        oli.line_total
    from orders o
    join order_line_items oli
        on oli.order_id = o.id
    join products p
        on p.id = oli.product_id
    join shops s
        on s.id = p.shop_id
    join skus sku
        on sku.id = oli.sku_id
    join categories c
        on c.id = sku.category_id
    where o.status in ('paid', 'shipped', 'delivered')
)
select
    date_trunc('month', order_date)::date as month_start,
    shop_name,
    category_name,
    sum(line_total) as revenue
from monetized_lines
group by 1, 2, 3
order by month_start, revenue desc;
```

## Example 3 - Avoid row multiplication

If you need order-level revenue and line-level product counts, aggregate each grain first.

```sql
with order_revenue as (
    select id as order_id, total_amount
    from orders
    where status in ('paid', 'shipped', 'delivered')
),
order_lines as (
    select order_id, count(*) as product_lines
    from order_line_items
    group by 1
)
select
    count(*) as orders,
    sum(total_amount) as revenue,
    avg(product_lines) as avg_product_lines
from order_revenue
join order_lines using (order_id);
```
