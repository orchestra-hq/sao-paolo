-- Deliberately not part of selector_x (model_a only). Exists so a scoped
-- freshness run has an out-of-scope source to skip.
select
    id,
    viewed_at
from {{ source('raw_unused', 'raw_page_views') }}
