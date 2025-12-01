with orders as (
    select * from {{ ref('stg_orders') }}
)
select
    order_id,
    customer_id,
    date_trunc('month', order_date) as order_month,
    sum(total_amount) over (partition by customer_id) as total_customer_spend
from orders
