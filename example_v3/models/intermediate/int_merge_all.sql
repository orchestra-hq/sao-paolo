{{
    config(
        materialized='incremental',
        freshness={
            "build_after": {
                "count": 10,
                "period": "minute",
                "updates_on": "all"
            }
        }
    )
}}

select * from {{ ref('int_merge') }}
union all
select * from {{ ref('stg_base') }}