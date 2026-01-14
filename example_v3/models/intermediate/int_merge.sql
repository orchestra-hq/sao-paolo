{{
    config(
        materialized='table',
        freshness={
            "build_after": {
                "count": 5,
                "period": "minute",
                "updates_on": "any"
            }
        }
    )
}}

select * from {{ ref('stg_fork_a') }}
union all
select * from {{ ref('stg_fork_b') }}