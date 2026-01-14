select * from {{ ref('stg_base') }}
union all
select 3 as id, 'fork_b' as name