select
    id,
    amount,
    {{ function('is_positive_int') }}(amount::text) as is_positive_amount,
    event_at,
    customer_id
from {{ ref('raw_events') }}
