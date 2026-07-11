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

## ✅ 3. Lake aberto roda o pipeline inteiro (sem `.duckdb`)

[folder 05 §2–§4](../../../experimentacoes/05-formato-storage-lake/RESULTADOS.md): tanto
**DuckLake** quanto **Delta** rodam Postgres→RAW (dlt) → CLEAN (dbt), overwrite + incremental,
e o Delta foi até o FalkorDB (100→120 nós ✅). Detalhes que pesam na escolha (§1 de
pontos-a-verificar):

- **DuckLake:** dlt e dbt escrevem **nativo**; catálogo SQL único; zero gambiarra.
- **Delta:** `dbt-duckdb` **não escreve Delta** — exige **ponte `write_deltalake`** OU o
  **plugin `store()`** (um `dbt run` faz overwrite + merge; 🛑 write duplo parquet→delta
  inevitável). Em compensação: maduro, interop Spark/Trino/Databricks.

## ✅ 4. Escrita concorrente → `concurrency limit = 1` no Prefect

DuckDB é single-writer (lock por processo); dois `dbt run` terminando juntos crasham o 2º
([folder 04](../../../experimentacoes/04-escrita-concorrente/RESULTADOS.md)). **Decisão:**
`global concurrency limit = 1` na tag de escrita. *(Fica menos crítico se o lake escolhido
tiver snapshot isolation — DuckLake/Delta.)*

## ✅ 5. Federation (no-ETL) funciona no essencial

[folder 06](../../../experimentacoes/06-federation-read-engine/RESULTADOS.md): uma engine lê
a CLEAN **do cliente** direto → FalkorDB, **sem ETL**. **DuckDB lê Delta e DuckLake**
in-process; **Trino lê Delta** (+ Iceberg/Snowflake via connectors) mas **não lê DuckLake**.
O grafo só recebe dicts — não sabe de onde vêm. Falta cravar contra **lake real em S3** e
resolver dedup/freshness → [tarefas/03-federation](tarefas/03-federation/).

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

---

## 💡 Aprendizado transversal

> **DuckDB é engine, não storage.** O `.duckdb` é um formato de arquivo *opcional* (como o
> `.sqlite`). Trocá-lo por Delta/DuckLake mantém a engine e abre o storage — destrava MinIO
> **e** tira o single-writer da borda CLEAN→grafo. E como o `memory-worker` recebe *dicts*
> e escreve *Cypher*, o caminho até o grafo **não muda** com o formato — o que torna a
> escolha do lake reversível e a federation plugável no mesmo caminho.
