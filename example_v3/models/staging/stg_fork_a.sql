select * from {{ ref('stg_base') }}
union all
select 32 as id, 'fork_a' as name