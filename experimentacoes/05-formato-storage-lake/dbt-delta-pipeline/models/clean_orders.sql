-- Abordagem "ponte": o dbt-duckdb NÃO escreve Delta, então materializa a CLEAN numa
-- tabela DuckDB scratch (efêmera) lendo a RAW em Delta via delta_scan. Depois do dbt,
-- o delta_pipeline.py lê `main.clean_orders` e faz write_deltalake/merge (a ponte Python).
{{ config(materialized='table') }}

select
    cast(id as bigint)            as id,
    cast(amount as double)        as amount,
    cast(updated_at as timestamp) as updated_at
from delta_scan('{{ env_var("RAW_ORDERS") }}')
-- dedup da RAW: última versão por id (padrão da clean layer)
qualify row_number() over (partition by id order by updated_at desc) = 1
