with transaction_summary as (

select
    ct.user_id,
    count(ct.transaction_id) total_transactions,
    sum(case when lt.is_fraudulent then 1 else 0 end) fraudulent_transactions,
    sum(case when not lt.is_fraudulent then 1 else 0 end) non_fraudulent_transactions

from AIRBYTE_DATABASE.STAGING.CUSTOMER_TRANSACTIONS ct
join AIRBYTE_DATABASE.STAGING.LABELED_TRANSACTIONS lt
on ct.transaction_id = lt.transaction_id
group by ct.user_id
)

select
    user_id,
    total_transactions,
    fraudulent_transactions,
    non_fraudulent_transactions,
    (fraudulent_transactions::FLOAT / total_transactions) * 100 risk_score

from transaction_summary