-- Example model demonstrating how to use a model from a dbt package
-- This references the date_dimension model from the example_package
with orders as (
    select * from {{ ref('stg_orders') }}
),
date_dim as (
    select * from {{ ref('my_utils', 'date_dimension') }}
)
select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount,
    d.year,
    d.quarter,
    d.month,
    d.day_of_week
from orders o
left join date_dim d 
    on date_trunc('day', o.order_date) = d.date_day

