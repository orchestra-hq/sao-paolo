select
    {{ dbt.date_trunc('day', 'event_at') }} as event_day,
    sum(amount) as total_amount,
    count(*) as event_count
from {{ ref('int_events_enriched') }}
group by {{ dbt.date_trunc('day', 'event_at') }}