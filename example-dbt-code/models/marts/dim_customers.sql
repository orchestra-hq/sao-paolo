with customers as (
    select * from {{ ref('stg_customers222') }}
)
select
    customer_id,
    customer_name,
    customer_email
from customers
