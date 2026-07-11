# 08 · Federation via ADBC — DuckDB lê o lake do cliente direto (sem Trino)

**Objetivo:** validar a **extensão ADBC do DuckDB** (community, `query-farm/adbc_scanner`)
como via de federação — o DuckDB vira **cliente ADBC** e lê Databricks/Snowflake/Postgres
**direto**, em Arrow, **sem Trino e sem plugin custom**. É a 3ª via da decisão
*engine de federação* (as outras: [`06`](../06-federation-read-engine/) DuckDB-vs-Trino
pra leitura, [`07`](../07-federation-dbt-clean/) o plugin Trino no dbt).

No teste a fonte é **Postgres** (papel do Databricks/Snowflake); trocar pra Databricks =
trocar o **driver ADBC** + a **URI**.

- Script: [`federate_adbc.py`](federate_adbc.py) — `read_adbc()` + JOIN com DuckLake.
- Achados e caveats: [`RESULTADOS.md`](RESULTADOS.md)

> Pré-req: Postgres do starter no ar (`:5432`). A extensão `adbc` e o driver
> `adbc-driver-postgresql` são instalados pelo próprio script/venv.
