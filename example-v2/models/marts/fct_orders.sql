with o as (select * from {{ ref('stg_wand_orders') }}),
w as (select * from {{ ref('dim_wizards') }}),
wd as (select * from {{ ref('stg_wands') }})
select o.*, w.name as wizard_name, wd.wand_type from o
left join w on o.wizard_id = w.id
left join wd on o.wand_id = wd.id
