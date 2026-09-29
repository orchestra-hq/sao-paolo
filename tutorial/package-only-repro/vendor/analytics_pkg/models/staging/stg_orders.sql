-- Owned by the package, so its original_file_path is
-- `models/staging/stg_orders.sql` -- relative to the package, not to the root
-- project, where no such file exists.
select * from {{ source('raw', 'raw_orders') }}
