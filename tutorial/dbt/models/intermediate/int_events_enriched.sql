select
    *,
    {{ audit_run_id() }} as _audit_run_id
from {{ ref('stg_events') }}
