select transaction_id, user_id, transaction_date, amount
from staging.customer_transactions
-- from AIRBYTE_DATABASE.STAGING.customer_transactions