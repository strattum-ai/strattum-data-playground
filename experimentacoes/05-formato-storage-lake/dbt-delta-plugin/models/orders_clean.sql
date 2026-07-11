-- Lê a RAW em Delta (delta_scan) e materializa a CLEAN DIRETO em Delta via o
-- plugin delta_writer. Espelha a arquitetura real: RAW=Delta -> CLEAN=Delta.
{{ config(
    materialized='external',
    plugin='delta_writer',
    location=env_var('EXP_TMP') ~ '/orders_clean.parquet',
    delta_table_path=env_var('EXP_CLEAN') ~ '/orders',
    delta_mode=env_var('EXP_MODE', 'overwrite'),
    delta_key='id'
) }}

select
    cast(id as bigint)              as id,
    trim(lower(customer_email))     as customer_email,
    cast(amount as double)          as amount,
    cast(updated_at as timestamp)   as updated_at
from delta_scan('{{ env_var("EXP_RAW") }}/orders')
-- dedup da RAW: mantém a última versão por id (padrão da clean layer)
qualify row_number() over (partition by id order by updated_at desc) = 1
