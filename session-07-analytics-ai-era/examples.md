# Examples - Session 7

## Example 1 - Prompt with enough context

```text
You are helping me write SQL for a PostgreSQL e-commerce schema.
Tables: orders, payments, order_line_items, products, shops, skus, categories.
The business question is: Are failed card payments contributing to lower weekly revenue?
Use order grain unless line-level category analysis is required.
Monetized orders are statuses paid, shipped, delivered.
Return SQL plus a short explanation of assumptions.
```

## Example 2 - AI draft to verify

Risky draft:

```sql
select
    date_trunc('week', o.created_at) as week_start,
    sum(o.total_amount) as revenue,
    count(oli.id) as items
from orders o
join order_line_items oli
    on oli.order_id = o.id
group by 1;
```

Verification notes:

- Joining line items changes the grain.
- Revenue may be double counted if summed after the join.
- Status filters are missing.
- Payment status is not included, so the question is not answered.

Safer version:

```sql
set search_path to app;

with order_level as (
    select
        date_trunc('week', o.created_at)::date as week_start,
        o.id as order_id,
        o.total_amount,
        p.provider,
        p.status as payment_status
    from orders o
    left join payments p
        on p.id = o.payment_id
    where o.status in ('paid', 'shipped', 'delivered')
)
select
    week_start,
    provider,
    payment_status,
    count(*) as orders,
    sum(total_amount) as revenue
from order_level
group by 1, 2, 3
order by week_start, provider, payment_status;
```

## Example 3 - Verification checklist

- Does every referenced table and column exist?
- What is the result grain?
- Are business filters included?
- Can any join multiply rows?
- Are metric definitions stated?
- Does the final answer overclaim beyond the data?
