# 07 · Ler do Databricks com DuckDB + ADBC — resultados

> **✅ VALIDADO.** O DuckDB leu uma tabela do **Databricks** via **ADBC**, in-process, sem ETL.
> É a prova de conceito da **federation** da 2.0 (fonte externa → DuckDB → [dbt / grafo]).

> Rodado em **DuckDB CLI** + extensão **`adbc`** (community) + driver **`databricks 0.1.2`**
> (ADBC Driver Foundry, instalado via `dbc install databricks`) + profile TOML. Fonte:
> **Databricks Serverless Starter Warehouse**, tabela `workspace.default.strattum_sample_orders`
> (200 linhas — o CSV que subimos).

## Como foi (o caminho que funcionou)

1. **Extensão DuckDB = cliente ADBC:** [`columnar-tech/duckdb-adbc-client`](https://github.com/columnar-tech/duckdb-adbc-client)
   ([artigo de anúncio, jul/2026](https://columnar.tech/blog/announcing-duckdb-adbc-extension/) — leitura recomendada) — `INSTALL adbc FROM community; LOAD adbc;`.
2. **Driver ADBC do Databricks:** `dbc install databricks` (CLI `dbc` da columnar-tech; instala o `.dylib` em `~/Library/Application Support/ADBC/Drivers`).
3. **Connection profile** (TOML, em `~/Library/Application Support/ADBC/Profiles/dbx.toml`):
   ```toml
   profile_version = 1
   driver = "databricks"
   [Options]
   uri = "databricks://token:<PAT>@dbc-xxxx.cloud.databricks.com:443/sql/1.0/warehouses/<id>"
   ```
   (host + http_path da aba *Connection details* do warehouse; PAT em *Settings → Developer → Access tokens*).
4. **Query:** `read_adbc('profile://dbx', '<SQL rodado no Databricks>')`.

## O que aconteceu de verdade

| Query | Resultado |
|---|---|
| `SELECT * … LIMIT 5` | ✅ 5 linhas, tipos certos (`id` int64, `amount` double, `updated_at` timestamp with tz) |
| `SELECT count(*)` | ✅ **200** |
| `SELECT status, count(*), sum(amount) … GROUP BY status` | ✅ paid **93** / 233034.83 · pending **38** / 85907.66 · canceled **37** / 84576.19 |

## O que isso prova

- **DuckDB lê Databricks via ADBC nativamente** — sem Trino, sem cópia pra RAW/CLEAN, sem JVM.
  O `read_adbc` empurra o `SELECT` interno **pro Databricks** (pushdown), e o DuckDB recebe o
  resultado como **tabela DuckDB (Arrow por baixo)** — dá pra filtrar/agregar/join com tabelas locais.
- **Fecha o buraco antigo** ("DuckDB não lê Snowflake/Databricks nativo"): a extensão ADBC +
  `dbc install <driver>` cobre Databricks, Snowflake, BigQuery, Postgres, etc.
- É o **mesmo caminho** que a federation precisa: `fonte do cliente → DuckDB → dict → grafo`
  (memory-worker) **ou** `→ dbt` (combinar com raw/enrichment).

## Caveats (o que ainda validar)

- **Driver `databricks` é 0.1.2 (novo)** — maturidade a acompanhar (tipos raros, pushdown de filtro).
- **`ATTACH` não tem predicate/projection pushdown** (ver limitações da extensão) — pra tabela
  grande, usar `read_adbc('profile://dbx', 'SELECT … WHERE updated_at > …')` (empurra o filtro).
- **Token = PAT** (secret) dentro do `uri` do profile → em produção vem do secrets store, e o
  profile é **gerado por run** (ver [tarefa 03](../../documents/arquitetura/2.0-lake-aberto/tarefas/03-federation/)).
- **Catalog/schema não são opções** do driver → qualificar na query (`workspace.default.tabela`).

## Próximos passos

- **Ida ao grafo:** `fed → memory_worker (ER + Cypher) → FalkorDB` (o worker recebe os dicts do `read_adbc`).
- **dbt:** um model lendo `read_adbc(...)` e fazendo JOIN com `raw`/`enrichment`.
- **Produção:** config na UI → salvar (config + secret) → **renderizar o profile por run** → dbt/worker usam. Detalhe em [tarefa 03](../../documents/arquitetura/2.0-lake-aberto/tarefas/03-federation/).
