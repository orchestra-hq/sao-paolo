-- A simple date dimension utility model
-- This demonstrates how to use a model from a dbt package
select
    date_day,
    year (date_day) as year,
    month (date_day) as month,
    day (date_day) as day,
    dayofweek (date_day) as day_of_week,
    quarter (date_day) as quarter
from
    (
        {% if target.type == 'databricks' %}
        select
            explode (
                sequence (
                    to_date ('2020-01-01'),
                    to_date ('2029-12-31'),
                    interval 1 day
                )
            ) as date_day
        {% elif target.type == 'snowflake' %}
        select
            dateadd(day, seq4(), '2020-01-01'::date) as date_day
        from
            table(generator(rowcount => 3653))
        where
            date_day <= '2029-12-31'::date
        {% else %}
        -- Default to Snowflake syntax for other adapters
        select
            dateadd(day, seq4(), '2020-01-01'::date) as date_day
        from
            table(generator(rowcount => 3653))
        where
            date_day <= '2029-12-31'::date
        {% endif %}
    )