select   
    id,
    cast(transaction_date as date) as transaction_date,
    amount,
    customer_name
from {{ ref('test_time_dimension') }}