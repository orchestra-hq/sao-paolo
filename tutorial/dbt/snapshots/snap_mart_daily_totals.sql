{% snapshot snap_mart_daily_totals %}

{{
    config(
        unique_key="event_day",
        strategy="check",
        check_cols=["total_amount", "event_count"],
    )
}}

select * from {{ ref("mart_daily_totals") }}

{% endsnapshot %}
