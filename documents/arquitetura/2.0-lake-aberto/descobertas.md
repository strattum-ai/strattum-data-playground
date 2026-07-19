# Descobertas — o que já testamos e concluímos

> **Findings dos experimentos ([`experimentacoes/`](../../../experimentacoes/)) e decisões
> tomadas.** O que ainda está aberto: [pontos-a-verificar](pontos-a-verificar.md). O backlog
> derivado: [tarefas/](tarefas/). Índice: [../README](../README.md).

Legenda: ✅ decidido · 📊 medido · 💡 aprendizado

---

## ✅ 1. Ingestão: `dlt + connectorx` (resolve o OOM)

O conector caseiro estoura a RAM em 1M+ linhas. Testado
([folder 01](../../../experimentacoes/01-ingestao-fonte-para-raw/RESULTADOS.md)):
`dlt (connectorx, 8 threads)` faz **100M linhas em ~3.5min, heap 49MB constante, RAM 1.4GB
plana**. Polars puro OOM em 100M; caseiro inviável acima de 1–2M.

- `write_disposition="merge"` + `primary_key` = upsert nativo (mata a duplicação RAW×CLEAN).
- `dlt.sources.incremental("updated_at")` mantém o cursor → **acaba o `connector_state` manual**.
- → vira [tarefas/02-conectores-dlt-connectorx](tarefas/02-conectores-dlt-connectorx/).

## ✅ 2. MinIO como lake aberto — RAW sim, `.duckdb` não

[folder 05 §1](../../../experimentacoes/05-formato-storage-lake/RESULTADOS.md): RAW
(Delta/Parquet) vai pro `s3://` sem drama (Polars e DuckDB leem/escrevem sem cópia).
**Ponto duro:** o `strattum.duckdb` (CLEAN) **não** foi feito pra object storage — sobre
`s3://` só abre read-only. Isso motivou DuckLake e Delta. → **object storage está decidido.**

## ✅ 3. Lake aberto — **decisão: DuckLake (catálogo em Postgres)**

[folder 05 §2–§4](../../../experimentacoes/05-formato-storage-lake/RESULTADOS.md): tanto
**DuckLake** quanto **Delta** rodam Postgres→RAW (dlt) → CLEAN (dbt), overwrite + incremental,
e o Delta foi até o FalkorDB (100→120 nós ✅). Comparados os dois:

- **DuckLake:** dlt e dbt escrevem **nativo**; **catálogo SQL único (Postgres)**; zero gambiarra.
- **Delta:** `dbt-duckdb` **não escreve Delta** — exige **ponte `write_deltalake`** OU o
  **plugin `store()`** (um `dbt run` faz overwrite + merge; 🛑 write duplo parquet→delta
  inevitável). Em compensação: maduro, interop Spark/Trino/Databricks.

**✅ Decisão: DuckLake, com o catálogo em Postgres** (o starter já roda Postgres). Motivos:
dbt/dlt escrevem nativo (sem ponte nem write duplo), catálogo único e o **catálogo em
Postgres dá snapshot isolation / evita a briga de concorrência** que era o problema do
`.duckdb`. O Delta fica como fallback caso um cliente exija interop com Spark/Databricks no
próprio lake. Trade-off assumido: DuckLake é novo (1.0) → maturidade **era** o [ponto a verificar](pontos-a-verificar.md); ✅ o [benchmark 2026-07-19](../../../BENCHMARK-LAKEHOUSE.md) rodou **2M + 1M linhas** de ponta a ponta (raw → clean → grafo) sem problema — resta só a concorrência multi-conector.

> ⚠️ **Cuidados que a decisão traz (rastreados na [tarefa 01](tarefas/01-lakehouse/)):**
> (1) o **`run_sql`** (skills-api) hoje só faz glob de **parquet** e **nem olha pra CLEAN** —
> com DuckLake tem que atachar o catálogo pra ver a clean; (2) o **memory-worker** passa a ler
> a CLEAN via `ATTACH 'ducklake:…'` (não mais o `.duckdb`); (3) **incrementalidade** depende
> das colunas certas na clean (cursor + chave).

## ✅ 4. Escrita concorrente → `concurrency limit = 1` no Prefect

DuckDB é single-writer (lock por processo); dois `dbt run` terminando juntos crasham o 2º
([folder 04](../../../experimentacoes/04-escrita-concorrente/RESULTADOS.md)). **Decisão:**
`global concurrency limit = 1` na tag de escrita. *(Fica menos crítico se o lake escolhido
tiver snapshot isolation — DuckLake/Delta.)*

## ✅ 5. Federation (no-ETL) — **decisão: DuckDB + ADBC**

[folder 06](../../../experimentacoes/06-federation-read-engine/RESULTADOS.md): uma engine lê
a CLEAN **do cliente** direto → FalkorDB, **sem ETL** (o grafo só recebe dicts — não sabe de
onde vêm). **Direção adotada: DuckDB + ADBC** (Arrow Database Connectivity) — via a extensão
[`duckdb-adbc-client`](https://columnar.tech/blog/announcing-duckdb-adbc-extension/) (columnar.tech,
jul/2026), que **transforma o DuckDB em cliente ADBC**: ele passa a ler **qualquer** sistema
com driver ADBC (Snowflake, Databricks, BigQuery, Redshift…) e recebe **Arrow zero-copy,
in-process**. Isso **elimina o buraco antigo** ("DuckDB não lê Snowflake/Databricks nativo") e
**dispensa o Trino**:

- **DuckDB in-process** já é o que o `memory-worker` usa (`CleanReader`) → **zero infra nova**.
- Lê **Delta/DuckLake por path** (`delta_scan`/attach) **e** Snowflake/Databricks **via ADBC**.
- **Não usamos Trino.** A extensão ADBC + `dbc install <driver>` cobre os warehouses fechados
  que eram a justificativa do Trino — sem cluster, sem metastore, sem infra nova.

✅ **VALIDADO contra o Databricks real** ([folder 07](../../../experimentacoes/07-databricks-adbc/RESULTADOS.md)):
DuckDB CLI + extensão `adbc` (community) + driver `databricks 0.1.2` (`dbc install databricks`) +
profile TOML → `read_adbc('profile://dbx', 'SELECT … FROM workspace.default.strattum_sample_orders')`
leu **200 linhas**, com o agregado (`GROUP BY status`) rodando **pushdown no Databricks** e o
DuckDB recebendo Arrow zero-copy. **Confirma o caminho `fonte externa → DuckDB → [grafo/dbt]`.**

🛑 Falta: **ida ao grafo** (fed → memory_worker → FalkorDB) e **wiring de produção** (config na
UI → secret + profile por run → dbt/worker). Acompanhar maturidade do driver (0.1.2, novo). →
[tarefas/03-federation](tarefas/03-federation/).

## ✅ 6. Nova camada: **enrichment (AI/LLM)** entre RAW e CLEAN

A flow `ai_enrichment_pipeline` ([strattum-data](../../../strattum-data/services/pipelines/src/flows/ai_enrichment_pipeline.py))
adiciona um estágio **`RAW → enrichment (LLM) → CLEAN`**: passa a RAW por chamadas de LLM
(rubrica + transcript → colunas/tabelas estruturadas no schema `enrichment`) **antes** do dbt
clean, com dependência dura (a clean só roda depois do enrichment). Se o cliente não escreve
o SQL da clean pra uma tabela enrichment, há `autogenerate_clean`. **Implicação pra 2.0:** o
lake ganha uma **terceira camada** (`enrichment`, também no DuckLake) — o `LakeStore` da
migração precisa controlá-la junto de RAW/CLEAN (ver [tarefa 01](tarefas/01-lakehouse/) e [migracao](tarefas/01-lakehouse/migracao.md)).

> 💡 **Sugestão a avaliar: fazer o enrichment em SQL/dbt com a extensão [`duckdb-ai`](https://github.com/leonardovida/duckdb-ai)** (`INSTALL ai FROM community`). Ela roda LLM **direto no DuckDB** — `ai_complete`, `ai_extract_record`/`ai_complete_json` (structured output), `ai_classify`, `ai_embed`, com **batch, cache e custo** embutidos, e suporta **Anthropic/OpenAI/Ollama/LiteLLM** (os mesmos providers que já usamos). Isso permitiria o `RAW → enrichment` virar um **model dbt** (`SELECT ai_extract_record(...) FROM raw`) em vez de um estágio Python separado — **uma engine só** (DuckDB) pra ingestão, enrichment, transform e federation.
>
> 🛑 **A verificar antes de adotar:** paridade com o `ai_enrichment` atual (cache por versão de rubrica/código, retry/dead-letter, budget de custo, dependência dura antes da clean); **maturidade** (v0.4.x, projeto novo); e se escreve bem no DuckLake. Vira ponto em [pontos-a-verificar](pontos-a-verificar.md) / [tarefa 01](tarefas/01-lakehouse/). Clone local: `~/Repos/duckdb-ai`.

---

## Findings medidos

### 📊 RAW → CLEAN escala bem no DuckDB (out-of-core)

Modelo com transforms + join + window, single-node (8 cores, 8 GB)
([folder 02](../../../experimentacoes/02-raw-para-clean/RESULTADOS.md)):

| linhas | tempo | pico RAM |
|---|---|---|
| 1M | 2,9 s | 0,9 GB |
| 10M | 23 s | 2,2 GB |
| 100M | ~13,5 min | 3,2 GB |

- 💡 **RAM plana** (~3,2 GB no 100M) — derrama pra disco em vez de OOM.
- 🛑 **Super-linear no 100M:** quando o sort não cabe na RAM, o spill de disco domina.
- ⚙️ Quase linear até **4 threads**; satura depois (o `threads: 4` do profile é sweet spot).

### 📊 Incremental no RAW→CLEAN é ~8× mais rápido

[folder 03](../../../experimentacoes/03-incremental/RESULTADOS.md):
`materialized='incremental'` dá **8× a 2% de delta** (ganho cresce com a tabela).
Pré-requisito: **`updated_at` na clean** (só 18/42 models têm hoje) →
[tarefas/01-lakehouse](tarefas/01-lakehouse/).

### 📊 Benchmark ponta-a-ponta no DuckLake (2M pg + 1M mongo) — 2026-07-19

O [BENCHMARK-LAKEHOUSE](../../../BENCHMARK-LAKEHOUSE.md) validou a arquitetura 2.0 no volume
real, num notebook de 8 GB (Docker VM 3,8 GB), usando os conectores/flows/dbt/worker de
produção (não um caminho à parte):

| Estágio | Volume | Tempo | Pico RAM |
|---|---:|---:|---:|
| Ingestão postgres → raw (DuckLake/Parquet) | 2.000.000 | 36,9 s | 272 MB |
| Ingestão mongo → raw (2 collections) | 1.000.500 | 25,0 s | 272 MB |
| Clean (dbt/DuckLake, com join Mongo×Mongo) | 3.000.500 | 3,8 s | 382 MB |
| Incremental (+150k, merge por PK) | 150.000 | 3,5 s | **0 duplicatas** |
| Grafo (fatia 290k nós + 300k arestas) | 590k elem | 126 s ¹ | 302 MB (FalkorDB) |

¹ Após otimizar o `execute_batch`: **1 `MERGE`/linha → `UNWIND` batch, 2,87×** (era 362 s).

- 💡 **DuckLake aguenta:** streaming ponta a ponta, RAM **constante** (~272 MB) independente do
  volume; o Parquet aberto (171 MB) ficou **mais compacto** que os bancos de origem (306 + 170 MB).
- 💡 **Incremental barato:** o delta custa o tamanho da **mudança**, não da base (100k em 2,2 s
  vs 2M em 37 s no bootstrap).
- 🛑 **Grafo é o estágio mais pesado** (limitado por RAM + velocidade de escrita) → detalhe +
  projeção por RAM em [pontos §1/§3](pontos-a-verificar.md).

---

## 💡 Aprendizado transversal

> **DuckDB é engine, não storage.** O `.duckdb` é um formato de arquivo *opcional* (como o
> `.sqlite`). Trocá-lo por Delta/DuckLake mantém a engine e abre o storage — destrava MinIO
> **e** tira o single-writer da borda CLEAN→grafo. E como o `memory-worker` recebe *dicts*
> e escreve *Cypher*, o caminho até o grafo **não muda** com o formato — o que torna a
> escolha do lake reversível e a federation plugável no mesmo caminho.
