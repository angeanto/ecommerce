{{ config(
    materialized='table'
) }}

/*
    Intermediate model summarising payment transactions by provider and status.
    It computes the total number of transactions and total amount processed for
    each combination of payment provider and payment status. A rank based on
    total amount is included to allow comparison across providers.
*/

with payments as (
    select
        *
    from {{ ref('stg_payments') }}
),

aggregated as (
    select
        provider
      , payment_status
      , count(*) as total_transactions
      , sum(payment_amount) as total_amount
    from payments
    group by 1, 2
)

select
    aggregated.provider
  , aggregated.payment_status
  , aggregated.total_transactions
  , aggregated.total_amount
  , dense_rank() over(partition by aggregated.payment_status order by aggregated.total_amount desc) as amount_rank_within_status
from aggregated