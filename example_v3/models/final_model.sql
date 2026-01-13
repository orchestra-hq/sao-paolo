with model_a as (
    select * from {{ ref('model_a') }}
),
model_b as (
    select * from {{ ref('model_b') }}
),
model_c as (
    select * from {{ ref('model_c') }}
),
model_d as (
    select * from {{ ref('model_d') }}
)
select 
    a.id as id_a,
    a.name as name_a,
    a.value as value_a,
    b.id as id_b,
    b.name as name_b,
    b.value as value_b,
    c.id as id_c,
    c.name as name_c,
    c.value as value_c,
    d.id as id_d,
    d.name as name_d,
    d.value as value_d
from model_a a
cross join model_b b
cross join model_c c
cross join model_d d
