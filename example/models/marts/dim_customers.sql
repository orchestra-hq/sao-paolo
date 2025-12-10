with customers as (
    select * from {{ ref('stg_customers') }}
)
select
    customer_id,
    customer_name,
    customer_email
from customers
