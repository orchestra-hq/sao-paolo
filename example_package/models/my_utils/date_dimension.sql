-- A simple date dimension utility model
-- This demonstrates how to use a model from a dbt package
select
    date_day,
    {% if target.type == 'bigquery' %}
    EXTRACT(YEAR FROM date_day) as year,
    EXTRACT(MONTH FROM date_day) as month,
    EXTRACT(DAY FROM date_day) as day,
    EXTRACT(DAYOFWEEK FROM date_day) as day_of_week,
    EXTRACT(QUARTER FROM date_day) as quarter
    {% elif target.type == 'databricks' %}
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    dayofweek(date_day) as day_of_week,
    quarter(date_day) as quarter
    {% elif target.type == 'snowflake' %}
    YEAR(date_day) as year,
    MONTH(date_day) as month,
    DAY(date_day) as day,
    DAYOFWEEK(date_day) as day_of_week,
    QUARTER(date_day) as quarter
    {% else %}
    -- Default to Snowflake syntax for other adapters
    YEAR(date_day) as year,
    MONTH(date_day) as month,
    DAY(date_day) as day,
    DAYOFWEEK(date_day) as day_of_week,
    QUARTER(date_day) as quarter
    {% endif %}
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
        {% elif target.type == 'bigquery' %}
        select
            date_day
        from
            unnest(generate_date_array('2020-01-01', '2029-12-31')) as date_day
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