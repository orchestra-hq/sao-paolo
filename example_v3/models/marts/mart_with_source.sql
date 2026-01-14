{{
    config(
        tags=['mart', 'final']
    )
}}

select 
    s.id,
    s.name,
    src.amount
from {{ ref('int_merge_all') }} s
left join {{ source('external', 'external_data') }} src
    on s.id = src.id