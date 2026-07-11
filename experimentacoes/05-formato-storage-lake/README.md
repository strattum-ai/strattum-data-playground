# 05 · Formato de storage do lake

**Objetivo:** decidir onde RAW e CLEAN aterrissam — `.duckdb` (hoje) vs **DuckLake** vs
**Delta**. Os três usam DuckDB como engine; muda só o storage. Alimenta a decisão de
storage da [arquitetura 2.0](../../docs/arquitetura/2.0-lake-aberto/pontos-a-verificar.md).

- Notebook consolidado: [`storage_lake.ipynb`](storage_lake.ipynb) (§1 MinIO · §2 DuckLake · §3 Delta · §4 dbt→Delta)
- Achados e comparação: [`RESULTADOS.md`](RESULTADOS.md)
- Scripts reutilizáveis: `delta_pipeline.py`, `dbt/delta_writer.py`, `dbt_delta_test.py`, `migrate_duckdb_to_lake.py`

> Funde as antigas investigações `05-armazenamento-minio`, `06-ducklake`,
> `07-delta-lake-tudo` e `12-dbt-escreve-delta` (eram o mesmo tema).
