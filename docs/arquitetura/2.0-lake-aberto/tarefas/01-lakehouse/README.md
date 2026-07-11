# 01 · Lakehouse — lake aberto + object storage + incremental + fim das duplicatas

> **Trocar o `.duckdb` monolítico por um lakehouse aberto** (DuckLake **ou** Delta, a
> definir), **mover o storage do volume Docker para o MinIO** (object storage) e, **no mesmo
> movimento**, resolver o incremental e as duplicatas — porque tudo tem a mesma raiz: sair
> do arquivo monolítico preso a filesystem e dar às tabelas as **colunas certas**
> (`updated_at` para watermark, `primary_key`/id estável para upsert).
>
> São o mesmo movimento porque o `.duckdb` é justamente o que **impede** o object storage
> (sobre `s3://` só abre read-only — [descobertas §2](../../descobertas.md)): trocar o
> formato do lake **é** o que destrava mover pro MinIO.
>
> Base: [descobertas §2–§4](../../descobertas.md) · comparação e experimentos:
> [folder 05](../../../../../experimentacoes/05-formato-storage-lake/RESULTADOS.md) · runbook:
> [migracao](../../migracao.md).

Legenda: 🛑 a fazer · ⚖️ decisão · 🔗 detalhe noutro doc

---

## ⚖️ Decisão a tomar: DuckLake vs Delta

Os dois tiram o `.duckdb`, destravam o object storage e dão **snapshot isolation** (matam o
single-writer). A diferença:

| | **DuckLake** | **Delta** |
|---|---|---|
| dlt escreve | ✅ nativo | ✅ nativo |
| **dbt escreve** | ✅ **nativo** (attach + materialized) | 🛑 ponte `write_deltalake` **ou** plugin `store()` (write duplo inevitável) |
| Catálogo | ✅ único (SQL: DuckDB/Postgres) | por-tabela; unificado só com Unity/Glue |
| Maturidade | novo (1.0) | maduro, amplo ecossistema |
| Interop / federação | poucos engines leem | ✅ Spark/Trino/Databricks/DuckDB — cliente Databricks = Delta |

- **Menos código / mais simples →** DuckLake.
- **Interop e federação (cliente Databricks/Delta) →** Delta.

**🛑 Como decidir:** rodar contra o **starter real** (Postgres + MinIO), não só local; medir
performance/custo vs `.duckdb`; avaliar maturidade (DuckLake é 1.0) e o catálogo em Postgres
sob concorrência de vários conectores.

---

## Tarefas

### A · Colunas certas na clean (pré-requisito de tudo)
- 🛑 **`updated_at` nas 24 clean models que faltam** (só 18/42 têm hoje). É o cursor de
  watermark — sem ele o grafo faz **full load** (round-trip por linha) e o incremental não liga.
- 🛑 **`primary_key` / id estável por model** — pra upsert (`write_delta`/merge) não duplicar.
- 🔗 Levantamento por conector: [pontos-a-verificar §2](../../pontos-a-verificar.md).

### B · Incremental no RAW→CLEAN
- 🛑 Trocar `materialized='table'` → `incremental` nas models (dá **8×** a 2% de delta —
  [descobertas](../../descobertas.md)) e **pular o `dbt run` quando o delta = 0**.
- Depende de (A) `updated_at`.

### C · Fim das duplicatas na borda clean→grafo
- 🛑 **Watermark:** o `memory_worker` faz **full scan** em clean sem `updated_at`
  ([reader.py `_build_select`](../../../../../strattum-ai/services/memory-worker/memory_worker/reader.py)).
  Resolve com (A).
- 🛑 **Idempotência do upsert:** o full scan duplica quando o `entity_id` cai no fallback
  `uuid.uuid4()` ([pipeline.py:459-480](../../../../../strattum-ai/services/memory-worker/memory_worker/pipeline.py#L459))
  ou quando dois eventos colidem em `event_id` (mesmo `source:join_value:timestamp` →
  [pipeline.py:695-726](../../../../../strattum-ai/services/memory-worker/memory_worker/pipeline.py#L695)).
  Garantir id determinístico/único.

### D · Volume Docker → MinIO (object storage), no mesmo movimento
- 🛑 Hoje RAW e CLEAN vivem no volume `strattum_data_local:/data`. Migrar o lake pro
  **MinIO** (`s3://`) — o RAW já vai sem drama; a CLEAN só passa a ir depois de sair do
  `.duckdb` (por isso é o mesmo movimento).
- 🛑 Apontar dlt (destino), dbt (profile) e os leitores (catalog-api, memory-worker via
  `httpfs`) pro `s3://` do MinIO em vez do path local. Credenciais via `AWS_*`.
- 🔗 O backfill já aceita `--out s3://…` — ver [migracao](../../migracao.md).

### E · Migração do storage (depois da decisão ⚖️)
- 🛑 Backfill `.duckdb` → lake + cutover dos leitores (catalog-api, memory-worker) + escrita
  da clean no lake. Plano completo (agnóstico Delta/DuckLake, com o passo do MinIO) em
  🔗 [migracao](../../migracao.md).
- 🛑 Serializar a escrita fica **menos crítico** com snapshot isolation, mas manter o
  `concurrency limit = 1` até validar.
