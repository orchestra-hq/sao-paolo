{{
    config(
        materialized='table',
        tags=['converging']
    )
}}

with path1 as (
    select * from {{ ref('stg_fork_a') }}
),
path2 as (
    select * from {{ ref('stg_fork_b') }}
),
both_paths as (
    select * from path1
    union all
    select * from path2
)
select 
    id,
    name,
    count(*) as path_count
from both_paths
group by id, name