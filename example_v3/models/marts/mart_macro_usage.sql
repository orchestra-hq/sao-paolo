{{
    config(
        tags=['macro_test']
    )
}}

select 
    {{ custom_macro('id') }} as processed_id,
    {{ custom_macro('name') }} as processed_name
from {{ ref('stg_base') }}