select
    id,
    event_at,
    amount,
    customer_id
from {{ ref('raw_events') }}
