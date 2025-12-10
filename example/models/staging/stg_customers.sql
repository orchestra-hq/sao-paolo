with source as (
    select * from {{ source('raw', 'src_customers') }}
)
select
    customer_id,
    customer_email,
    customer_name
from source
