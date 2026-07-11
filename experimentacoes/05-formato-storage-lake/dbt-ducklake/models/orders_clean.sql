-- RAW (lake.raw.orders) -> CLEAN (lake.clean.orders_clean), tudo DENTRO do DuckLake.
-- database='lake' = catálogo DuckLake anexado; dbt materializa nativo (sem ponte).
-- full-refresh = overwrite; run normal = incremental (só o delta por updated_at).
{{ config(materialized='incremental', unique_key='id', database='lake', schema='clean') }}

select
    id,
    cast(updated_at as timestamp) as updated_at,
    amount,
    lower(trim(email))            as email     -- transform de exemplo (clean)
from lake.raw.orders
{% if is_incremental() %}
where cast(updated_at as timestamp) > (
    select coalesce(max(updated_at), timestamp '1900-01-01') from {{ this }}
)
{% endif %}
