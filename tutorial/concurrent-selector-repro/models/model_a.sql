-- A: fed directly by the source so its freshness is driven by source data.
-- Reference source() (not the seed) so Orchestra attaches the source to this
-- node and tracks its freshness in state.
select
    id,
    amount,
    event_at,
    customer_id
from {{ source('raw', 'raw_events') }}
