# Examples - Session 6

## Example 1 - Metric contract

Metric: Captured Revenue

- Definition: Sum of payment amount where payment status is captured.
- Grain: Payment transaction.
- Owner: Finance Analytics.
- Source: `app.payments`, dbt model `rep_payment_summary`.
- Refresh: Daily.
- Tests: Non-null provider, non-null payment status, non-negative amount.
- Known exclusions: Failed, cancelled, refunded, and pending payments.

## Example 2 - Governance SQL check

```sql
set search_path to app;

select
    p.status,
    count(*) as transactions,
    sum(p.amount) as amount
from payments p
group by 1
order by amount desc;
```

## Example 3 - dbt quality test

```yaml
models:
  - name: rep_main_kpis_per_shop
    description: Shop-level marketplace KPIs used for executive reporting.
    columns:
      - name: shop_id
        tests:
          - not_null
          - unique
      - name: total_revenue
        tests:
          - not_null
```

## Example 4 - Dashboard lifecycle review

Review each dashboard quarterly:

- Who uses it?
- What decision does it support?
- Which metrics are trusted and which are disputed?
- Which charts have not been used in 90 days?
- Which model or definition changes are pending?
