# Examples - Session 1

These examples use the e-commerce database to demonstrate how a modern analyst translates vague work into a clear decision process.

## Example 1 - Turn a vague request into an analytical brief

Vague request:

"Can you check why revenue looks down?"

Better analyst framing:

- Decision: Should Growth investigate campaign quality, checkout issues, or merchant assortment?
- Owner: Head of Growth.
- Timebox: Initial readout by tomorrow morning.
- Metric: Paid or delivered order revenue, compared weekly.
- Segments: Payment status, shop, category, and returning vs new users.

Starter SQL:

```sql
set search_path to app;

with monetized_orders as (
    select
        date_trunc('week', o.created_at)::date as week_start,
        o.id as order_id,
        o.user_id,
        o.status as order_status,
        p.status as payment_status,
        o.total_amount
    from orders o
    left join payments p
        on p.id = o.payment_id
    where o.created_at >= current_date - interval '8 weeks'
)
select
    week_start,
    count(*) as order_count,
    sum(total_amount) as revenue,
    count(*) filter (where payment_status = 'failed') as failed_payments,
    round(
        count(*) filter (where payment_status = 'failed')::numeric
        / nullif(count(*), 0),
        4
    ) as failed_payment_rate
from monetized_orders
where order_status in ('paid', 'shipped', 'delivered')
group by 1
order by 1;
```

## Example 2 - Intake notes that prevent rework

Use this before building:

- Business question: Which decision is blocked?
- Current belief: What does the stakeholder think is happening?
- Metric definition: What exactly counts as revenue?
- Time grain: Daily, weekly, monthly, cohort, or event-level?
- Comparison: Prior period, target, forecast, or peer segment?
- Action threshold: What result would change the decision?

## Example 3 - Communicate a first readout

A concise update:

"I checked paid and delivered revenue by week. The drop is concentrated in orders with failed payments, especially card payments. I am checking whether this is provider-specific or category-specific before recommending a fix."
