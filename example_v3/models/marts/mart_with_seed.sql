select 
    m.id,
    m.name,
    s.seed_value
from {{ ref('int_merge') }} m
cross join {{ ref('seed_data') }} s