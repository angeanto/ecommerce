# Examples - Session 3

These examples use the dbt project in `ecommerce_db`.

## Example 1 - Trace the pipeline

Source declaration:

```yaml
sources:
  - name: pg
    schema: raw_app
    tables:
      - name: orders
      - name: payments
```

Staging model pattern:

```sql
-- models/staging/stg_orders.sql
select
    id as order_id,
    user_id,
    status as order_status,
    total_amount as order_total_amount,
    payment_id,
    created_at as order_created_at
from {{ source('pg', 'orders') }}
```

Reporting model pattern:

```sql
select
    provider,
    payment_status,
    total_transactions,
    total_amount,
    avg_transaction_value
from {{ ref('rep_payment_summary') }}
```

## Example 2 - Add tests for a model

```yaml
models:
  - name: rep_payment_summary
    description: Payment performance by provider and status.
    columns:
      - name: provider
        tests:
          - not_null
      - name: payment_status
        tests:
          - not_null
      - name: total_transactions
        tests:
          - not_null
```

## Example 3 - Git workflow for a model change

```bash
git checkout -b feature/payment-summary-tests
dbt run --select rep_payment_summary
dbt test --select rep_payment_summary
git add models/reporting/schema.yml
git commit -m "add payment summary tests"
git push -u origin feature/payment-summary-tests
```

## Example 4 - Analyst production checklist

- Is the source declared?
- Is staging doing only cleanup and renaming?
- Is business logic reusable in intermediate models?
- Is the reporting model documented?
- Are grain and uniqueness tested?
