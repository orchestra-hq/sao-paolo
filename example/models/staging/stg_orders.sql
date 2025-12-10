with source as (
    select * from {{ source('raw', 'src_orders') }}
)
select
    order_id,
    customer_id,
    order_date,
    total_amount
from source
