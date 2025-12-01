with orders as (
    select * from {{ ref('stg_orders') }}
)
select
    order_id_xxx,
    customer_id,
    date_trunc('month', order_date) as order_month_v2,
    sum(total_amount) over (partition by customer_id) as total_customer_spend
from orders
