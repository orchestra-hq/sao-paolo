select * from {{ ref('mart_ephemeral_dep') }}
union all
select 999 as id, 'ephemeral_test' as name