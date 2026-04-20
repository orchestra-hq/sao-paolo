select
    date_trunc('day', event_at)::date as event_day,
    sum(amount) as total_amount,
    count(*) as event_count
from {{ ref('int_events_enriched') }}
group by 1
