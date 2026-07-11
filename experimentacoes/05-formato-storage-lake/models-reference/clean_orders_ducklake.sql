-- RAW -> CLEAN no DuckLake.
-- `dbt run --full-refresh` reconstrói tudo (overwrite); `dbt run` processa só o delta.
{{ config(materialized='incremental', unique_key='id', database='lake', schema='clean') }}

select
    id,
    cast(updated_at as timestamp) as updated_at,
    amount
from lake.raw.orders
{% if is_incremental() %}
where cast(updated_at as timestamp) > (select coalesce(max(updated_at), timestamp '1900-01-01') from {{ this }})
{% endif %}
