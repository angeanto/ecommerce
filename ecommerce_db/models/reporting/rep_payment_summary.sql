{{ config(
    materialized='table'
) }}

/*
    Reporting model that enhances payment summary statistics with derived
    metrics. It calculates the average transaction value and the share of
    transactions by status within each provider. Window functions further
    provide lag/lead and cumulative transaction counts across providers.
*/

with payment_summary as (
    select
        *
    from {{ ref('int_payment_summary') }}
),

by_provider as (
    select
        provider
      , sum(total_transactions) as total_transactions_by_provider
    from payment_summary
    group by 1
)

select
    payment_summary.provider
  , payment_summary.payment_status
  , payment_summary.total_transactions
  , payment_summary.total_amount
  , case when payment_summary.total_transactions > 0 then payment_summary.total_amount / payment_summary.total_transactions else null end as avg_transaction_value
  , case when by_provider.total_transactions_by_provider > 0 then payment_summary.total_transactions / by_provider.total_transactions_by_provider else null end as status_transaction_share
  , lag(payment_summary.total_amount) over(order by payment_summary.total_amount desc) as previous_total_amount
  , lead(payment_summary.total_amount) over(order by payment_summary.total_amount desc) as next_total_amount
  , sum(payment_summary.total_amount) over(order by payment_summary.total_amount desc rows between unbounded preceding and current row) as cumulative_total_amount
from payment_summary
join by_provider
    on payment_summary.provider = by_provider.provider