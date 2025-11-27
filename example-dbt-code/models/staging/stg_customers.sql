with source as (
    select * from {{ source('raw', 'src_customers') }}
)
select
    customer_id,
    customer_name,
    customer_email
from source
