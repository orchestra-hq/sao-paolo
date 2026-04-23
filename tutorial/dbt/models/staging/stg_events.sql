select
    id,
    amount,
    event_at,
    customer_id
from {{ ref('raw_events') }}
