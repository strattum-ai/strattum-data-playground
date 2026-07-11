# 07 · Federation → CLEAN via dbt (DuckLake × Trino)

**Objetivo:** provar que o **dbt** consegue montar uma tabela **CLEAN nova** juntando
uma tabela **da nossa RAW (DuckLake)** com uma tabela **da camada federada (Trino
lendo uma fonte externa)** — e materializar o resultado **de volta na DuckLake**.

É o caso "federação na escrita": diferente do [`06`](../06-federation-read-engine/)
(federação → grafo, sem ETL), aqui a federação alimenta um **transform dbt** que cria
uma tabela clean. O Trino, no teste, lê um **Postgres**; em produção leria
**Databricks/Snowflake** — troca só de catálogo.

- Notebook: [`federation_dbt.ipynb`](federation_dbt.ipynb) — enxuto, roda ponta a ponta.
- Projeto dbt: [`dbt_fed/`](dbt_fed/) — `plugins/trino_source.py` (plugin que serve
  qualquer tabela federada do Trino) + `models/sources.yml` (declara as 2 fontes:
  DuckLake + Trino) + `models/clean_order_lines.sql` (o JOIN em SQL puro → DuckLake).
- Achados e caveats: [`RESULTADOS.md`](RESULTADOS.md)

> Requer Docker (`trinodb/trino`) e o Postgres do starter no ar (`:5432`).
