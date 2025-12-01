with orders as (
    select * from {{ ref('int_orders') }}
),
customers as (
    select * from {{ ref('dim_customers') }}
)
select
    c.customer_id,
    c.customer_name,
    o.order_id,
    o.order_month,
    -- o.total_customer_spend
    o.total_customer_spend
from orders o
join customers c on o.customer_id = c.customer_id
