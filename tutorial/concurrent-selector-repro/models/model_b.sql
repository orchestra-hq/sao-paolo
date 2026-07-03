-- B: depends only on A. No source of its own and no build_after config, so its
-- only signal to rebuild is "A has newer output than the last time I ran".
select
    id,
    amount,
    event_at,
    customer_id
from {{ ref('model_a') }}
