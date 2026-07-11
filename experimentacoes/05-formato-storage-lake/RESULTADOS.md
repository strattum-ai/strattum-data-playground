# 05 · Formato de storage do lake — resultados

> Consolida 4 investigações do mesmo tema: **onde RAW e CLEAN aterrissam.** Os três
> candidatos usam **DuckDB como engine**; diferem só no **storage**.
> Rodado em **DuckDB 1.5.4 · dlt 1.28.1 · dbt-duckdb 1.10.1 · deltalake 1.6.1 · Prefect 3.7**.
> Notebook: [`storage_lake.ipynb`](storage_lake.ipynb).

## A pergunta

Hoje RAW = Delta e **CLEAN = `.duckdb`** (arquivo monolítico). O `.duckdb`:

- 🛑 **não vai pro object storage** — sobre `s3://` só abre **read-only** (§1);
- 🛑 **é single-writer** — file lock por processo, reencena a briga de lock na borda CLEAN→grafo.

Sair dele destrava MinIO **e** dá snapshot isolation. Dois candidatos abertos: **DuckLake** e **Delta**.

## O que rodou

| # | Teste | Veredito |
|---|---|---|
| §1 | `.duckdb` sobre MinIO `s3://` | 🛑 **read-only** — RAW (Delta/Parquet) vai sem drama; o `.duckdb` da CLEAN não |
| §2 | Pipeline inteiro em **DuckLake** (dlt→RAW→dbt→CLEAN, overwrite + incremental) | ✅ dlt e dbt **nativos**; catálogo SQL único; zero gambiarra |
| §3 | Pipeline inteiro em **Delta** até o FalkorDB | ✅ roda (100→120 nós); 🛑 `dbt-duckdb` **não escreve Delta** → ponte `write_deltalake` |
| §4 | **dbt escreve Delta direto** via plugin `store()` | ✅ um `dbt run` faz overwrite + merge; 🛑 write duplo (parquet temp → Delta) inevitável |

## Comparação (a decisão)

| | **`.duckdb`** (hoje) | **DuckLake** | **Delta** |
|---|---|---|---|
| MinIO / `s3://` | 🛑 read-only | ✅ nativo | ✅ nativo |
| Single-writer | 🛑 lock por processo | ✅ snapshot isolation | ✅ snapshot isolation |
| **dbt escreve** | ✅ | ✅ **nativo** (attach) | 🛑 ponte OU plugin `store()` (write duplo) |
| Catálogo | ✅ `information_schema` | ✅ único (SQL) | por-tabela; unificado só com Unity/Glue |
| Maturidade | — | novo (1.0) | maduro (Spark/Trino/Databricks) |
| Interop / federação | só DuckDB | poucos engines | ✅ cliente Databricks = Delta |

## Conclusão

- **Menos código / mais simples →** DuckLake (dbt escreve sem ponte, catálogo único).
- **Interop e federação (cliente com Databricks/Delta, ler com Trino/Spark) →** Delta,
  aceitando a ponte/plugin na CLEAN.
- **Decisão final: a confirmar** contra o **starter real** (Postgres + MinIO), medindo
  performance/custo. Ver [pontos-a-verificar §1](../../docs/arquitetura/2.0-lake-aberto/pontos-a-verificar.md).

## Artefatos preservados

| Arquivo | O que é |
|---|---|
| [`delta_pipeline.py`](delta_pipeline.py) | Pipeline Delta completo (dlt→RAW→dbt+ponte→CLEAN→FalkorDB), overwrite + incremental |
| [`dbt-delta-plugin/delta_writer.py`](dbt-delta-plugin/delta_writer.py) | Plugin dbt-duckdb que escreve Delta no `store()` (§4) |
| [`dbt_delta_test.py`](dbt_delta_test.py) | Teste A/B do plugin (overwrite 100 → merge 115) |
| [`migrate_duckdb_to_lake.py`](migrate_duckdb_to_lake.py) | Backfill `.duckdb` → lake (usado no runbook de migração) |
| [`dbt-delta-plugin/models/`](dbt-delta-plugin/models/) | Modelo `orders_clean` (variante Delta/plugin); DuckLake em models-reference/ |
