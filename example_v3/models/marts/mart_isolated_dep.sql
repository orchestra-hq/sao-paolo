select * from {{ ref('mart_isolated') }}
union all
select 101 as id, 'isolated_dep' as name