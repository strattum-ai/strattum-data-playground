# 06 · Federation (no-ETL) — read-engine

**Objetivo:** para empresas **com lake** (Snowflake/Databricks), ler a CLEAN do cliente
**direto → grafo**, sem ETL. Qual engine — **DuckDB** ou **Trino**? Alimenta a pergunta
*duckdb vs trino* da [arquitetura 2.0](../../docs/arquitetura/2.0-lake-aberto/pontos-a-verificar.md).

- Notebook: [`federation.ipynb`](federation.ipynb)
- Achados e caveats: [`RESULTADOS.md`](RESULTADOS.md)
- Scripts: [`federate_duckdb.py`](federate_duckdb.py), [`federate_trino.py`](federate_trino.py)

> Era a antiga `13-federation-clean-grafo`.
