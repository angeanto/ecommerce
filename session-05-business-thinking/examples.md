# Examples - Session 5

## Example 1 - KPI tree for marketplace revenue

Strategic objective:

"Grow profitable marketplace revenue without increasing payment risk."

KPI tree:

- Net revenue.
- Orders.
- Average order value.
- Active shops.
- Category mix.
- Payment success rate.
- Refund rate.

## Example 2 - Driver query

```sql
set search_path to app;

with order_metrics as (
    select
        date_trunc('week', o.created_at)::date as week_start,
        count(*) as orders,
        sum(o.total_amount) as revenue,
        avg(o.total_amount) as avg_order_value,
        count(*) filter (where p.status = 'captured') as captured_payments,
        count(*) filter (where p.status = 'failed') as failed_payments,
        count(*) filter (where o.status = 'refunded') as refunded_orders
    from orders o
    left join payments p
        on p.id = o.payment_id
    group by 1
)
select
    week_start,
    revenue,
    orders,
    avg_order_value,
    round(captured_payments::numeric / nullif(orders, 0), 4) as payment_capture_rate,
    round(failed_payments::numeric / nullif(orders, 0), 4) as payment_failure_rate,
    round(refunded_orders::numeric / nullif(orders, 0), 4) as refund_rate
from order_metrics
order by week_start;
```

## Example 3 - Stakeholder conflict

Growth may want higher order volume. Finance may care about refund risk. Operations may care about merchant reliability.

Recommendation framing:

"The growth opportunity is strongest in high-revenue categories, but the recommendation should exclude shops with high refund or failed-payment patterns until reliability improves."

## Example 4 - Trade-off-aware segment

```sql
select
    s.name as shop_name,
    count(distinct o.id) as orders,
    sum(oli.line_total) as revenue,
    count(*) filter (where o.status = 'refunded') as refunded_lines
from orders o
join order_line_items oli
    on oli.order_id = o.id
join products p
    on p.id = oli.product_id
join shops s
    on s.id = p.shop_id
group by 1
order by revenue desc
limit 20;
```
