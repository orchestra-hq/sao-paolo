{% snapshot simple_snapshot %}

{{
    config(
        target_schema='test_ops',
        unique_key='id',
        strategy='check',
        check_cols=['name'],
    )
}}

select *
from {{ ref('stg_base') }}

{% endsnapshot %}

