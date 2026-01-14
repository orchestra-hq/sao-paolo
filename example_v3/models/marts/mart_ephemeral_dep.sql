{{
    config(
        materialized='ephemeral'
    )
}}

select * from {{ ref('int_merge') }}
where id > 1