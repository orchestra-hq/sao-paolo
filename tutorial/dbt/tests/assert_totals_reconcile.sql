with staging as (
    select sum(amount) as total from {{ ref('stg_events') }}
),
mart as (
    select sum(total_amount) as total from {{ ref('mart_daily_totals') }}
)
select
    staging.total as staging_total,
    mart.total as mart_total
from staging
cross join mart
where staging.total <> mart.total
