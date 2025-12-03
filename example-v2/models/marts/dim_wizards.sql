with w as (select * from {{ ref('stg_wizards') }}),
worlds as (select * from {{ ref('stg_worlds') }})
select w.*, worlds.name as world_name from w left join worlds on w.world_id = worlds.id
