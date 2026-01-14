{{
    config(
        materialized='view'
    )
}}

select 
    id,
    name,
    'base' as ref_type
from {{ ref('stg_base') }}