-- CLEAN: enriquece nossos pedidos (RAW em DuckLake) com o mestre de produtos
-- (fonte FEDERADA via Trino → plugin). Duas `source()`, JOIN puro em SQL na engine
-- DuckDB, materializado de volta na DuckLake (database=lake).
{{ config(materialized='table', database='lake') }}

select
    o.order_id,
    o.customer_email,
    o.product_sku,
    p.product_name,
    p.category,
    o.qty,
    p.unit_price,
    o.qty * p.unit_price          as line_total,
    o.order_ts
from {{ source('raw_lake', 'orders') }} o
join {{ source('federated', 'products') }} p
  on o.product_sku = p.sku
order by o.order_id
