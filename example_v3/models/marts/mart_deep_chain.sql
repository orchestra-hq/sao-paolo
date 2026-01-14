{{
    config(
        freshness={
            "build_after": {
                "count": 1,
                "period": "hour"
            }
        }
    )
}}

with level1 as (
    select * from {{ ref('model_a') }}
),
level2 as (
    select * from level1
    union all
    select * from {{ ref('model_b') }}
),
level3 as (
    select * from level2
    union all
    select * from {{ ref('model_c') }}
)
select * from level3
union all
select * from {{ ref('model_d') }}