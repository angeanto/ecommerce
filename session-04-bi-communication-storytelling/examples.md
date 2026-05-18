# Examples - Session 4

Use these queries as dashboard sources or as the basis for a written readout.

## Example 1 - Executive KPI trend

```sql
set search_path to app;

with daily as (
    select
        o.created_at::date as order_date,
        count(*) as orders,
        sum(o.total_amount) as revenue,
        count(*) filter (where p.status = 'failed') as failed_payments
    from orders o
    left join payments p
        on p.id = o.payment_id
    where o.status in ('paid', 'shipped', 'delivered')
    group by 1
)
select
    order_date,
    revenue,
    orders,
    failed_payments,
    round(failed_payments::numeric / nullif(orders, 0), 4) as failed_payment_rate,
    avg(revenue) over (
        order by order_date
        rows between 6 preceding and current row
    ) as revenue_7d_avg
from daily
order by order_date;
```

## Example 2 - Dashboard sections

Monitoring view:

- Revenue, orders, failed payment rate, active shops.
- Trend against prior 7 days and prior 4 weeks.

Diagnostic view:

- Payment provider status.
- Revenue by shop.
- Revenue by category.
- New vs returning users.

Executive readout:

- What changed.
- Why it likely changed.
- What action is recommended.
- What risk remains.

## Example 3 - Insight phrasing

Weak:

"Revenue is down and card payments are high."

Better:

"Revenue fell in the latest week, and the drop is concentrated in orders linked to failed card payments. The next action is to confirm provider-level failure rates before treating the issue as a demand problem."
