# 01 · Lakehouse — DuckLake (catálogo Postgres) + object storage + incremental + fim das duplicatas

> **Trocar o `.duckdb` monolítico pelo DuckLake** (catálogo em Postgres, dados em object
> storage), **mover o storage do volume Docker para o MinIO** e, **no mesmo movimento**,
> resolver o incremental e as duplicatas — porque tudo tem a mesma raiz: sair do arquivo
> monolítico preso a filesystem e dar às tabelas as **colunas certas** (`updated_at` para
> watermark, `primary_key`/id estável para upsert).
>
> São o mesmo movimento porque o `.duckdb` é justamente o que **impede** o object storage
> (sobre `s3://` só abre read-only — [descobertas §2](../../descobertas.md)): trocar o
> formato do lake **é** o que destrava mover pro MinIO.
>
> Base: [descobertas §2–§4](../../descobertas.md) · comparação e experimentos:
> [folder 05](../../../../../experimentacoes/05-formato-storage-lake/RESULTADOS.md) · runbook:
> [migracao](migracao.md).

Legenda: 🛑 a fazer · ⚖️ decisão · 🔗 detalhe noutro doc

---

## ✅ Decisão: DuckLake com catálogo em Postgres

Escolhido o **DuckLake** (o Delta fica como fallback p/ interop Spark/Databricks). Os dois
tiram o `.duckdb` e dão snapshot isolation; o DuckLake ganha por:

| | **DuckLake** (escolhido) | Delta (fallback) |
|---|---|---|
| dbt/dlt escrevem | ✅ **nativo** (attach + materialized) | dlt sim; dbt 🛑 ponte/plugin (write duplo) |
| Catálogo | ✅ **único, em Postgres** (o starter já roda Postgres) | por-tabela; unificado só com Unity/Glue |
| Concorrência | ✅ o catálogo Postgres serializa/isola → **mata o single-writer** | snapshot isolation (protocolo no storage) |
| Maturidade | 🛑 novo (1.0) → [ponto a verificar](../../pontos-a-verificar.md) | maduro |

> ⚠️ **Cuidados que a decisão traz** (não são bloqueio, são o que a migração tem que tratar):
> - **`run_sql` (skills-api) hoje só faz glob de parquet e nem olha a CLEAN** → com DuckLake,
>   o `run_sql` precisa **atachar o catálogo** (`ATTACH 'ducklake:…'`) pra enxergar a clean.
> - **memory-worker → grafo**: o `CleanReader` passa a ler via `ATTACH ducklake:` em vez do
>   `.duckdb`; o resto (ER → Cypher) não muda.
> - **incrementalidade**: depende das **colunas certas** na clean (cursor + chave) — que vêm
>   do **config do conector** (ver [tarefa 02](../02-conectores-dlt-connectorx/) e item A abaixo).
>
> **🛑 A confirmar (não muda a decisão):** rodar contra o **starter real** (Postgres + MinIO),
> medir perf/custo vs `.duckdb`, e o **catálogo Postgres sob concorrência** de vários conectores.

## 🆕 Camada `enrichment` (AI/LLM) entra no lake também

O pipeline ganhou um estágio **`RAW → enrichment (LLM) → CLEAN`**
([ai_enrichment_pipeline](../../../../../strattum-data/services/pipelines/src/flows/ai_enrichment_pipeline.py),
[descobertas §6](../../descobertas.md)): a RAW passa por LLM (schema `enrichment`) antes do dbt
clean. **Implicação:** o lake tem **três camadas** — `raw`, `enrichment`, `clean` — todas no
DuckLake. A classe de acesso ao lake (abaixo) precisa tratar as três de forma uniforme.

---

## Tarefas

### A · Colunas certas na clean (pré-requisito de tudo) — **vêm do config do conector**
- 🛑 O **usuário/config do conector declara** as **colunas de incrementalidade**: um **cursor**
  (`updated_at`/timestamp/data) e uma **chave** (`primary_key`/id estável). Sem cursor o grafo
  faz **full load**; sem chave o upsert **duplica**.
- 🛑 O **modo de escrita** (`overwrite` vs `incremental`) também sai do config: se o conector
  declara cursor+chave → incremental (merge); senão → overwrite (full refresh). Ver [tarefa 02](../02-conectores-dlt-connectorx/).
- 🛑 Projetar `updated_at` nas 24 clean models que faltam (só 18/42 têm hoje).
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
- 🔗 O backfill já aceita `--out s3://…` — ver [migracao](migracao.md).

### E · Migração do storage + a classe `LakeStore`

A migração (que antes vivia num doc à parte) é **desta tarefa**. Backfill `.duckdb` → DuckLake
+ cutover dos leitores (catalog-api, memory-worker, skills-api) + escrita da clean no lake. O
runbook detalhado continua em 🔗 [migracao](migracao.md); aqui fica o **desenho da peça
central**.

**🛑 Ideia: uma classe `LakeStore` — o único ponto que fala com o lake E com a federation.**
Hoje o acesso ao storage está espalhado (`write_delta` nos flows, `CleanReader` no worker,
`_duckdb_path` no catalog-api, glob de parquet na skills-api). A migração é a chance de
**centralizar** numa classe que recebe uma **fonte** (uma camada nossa **ou** uma fonte
federada) e faz todo o controle — **e é aqui que já embutimos o requisito de federation**
(mesmo a implementação dela sendo da [tarefa 03](../03-federation/)): quem lê a CLEAN pro grafo
tem que ler uma fonte federada com **a mesma chamada**.

```python
class LakeStore:
    """Único ponto de leitura/escrita. Abstrai formato, catálogo, camada E origem (lake vs federation)."""
    def __init__(self, catalog: str, storage: str):   # catalog=Postgres DSN, storage=s3://…
        self._con = duckdb.connect()
        self._con.execute("INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;")
        self._con.execute(f"ATTACH 'ducklake:{catalog}' AS lake")

    # ---- registra uma fonte externa (federation) via ADBC — Snowflake/Databricks ----
    def attach_federation(self, name: str, *, adbc: dict | None = None, delta_path: str | None = None):
        # cria uma view 'fed.<name>' sobre a fonte externa (ADBC → Arrow, ou delta_scan/iceberg_scan)
        ...

    # ---- LEITURA UNIFICADA: 'source' pode ser camada nossa OU federation ----
    def read(self, source, *, columns=None, watermark=None, cursor="updated_at"):
        # source = ("clean", "dim_companies")        -> lake.clean.dim_companies
        #        | ("enrichment", "x") | ("raw", "y")
        #        | ("fed", "salesforce_accounts")    -> fonte federada (ADBC/scan)
        # devolve Arrow/dicts; aplica WHERE {cursor} > watermark se houver. MESMA saída p/ os dois.
        ...

    # ---- escrita (dlt/dbt bridge, enrichment) ----
    def write(self, layer, table, arrow, *, mode="overwrite", primary_key=None): ...  # layer ∈ {raw,enrichment,clean}
    def list_tables(self, layer): ...       # trivial no DuckLake (catálogo único)
    def snapshot(self, source, table): ...  # snapshot estável p/ o worker ler enquanto o dbt escreve
```

**Requisito-chave (por isso desenhamos agora):** `read()` é **agnóstico de origem** — o
`memory-worker` chama `read(("clean", t))` ou `read(("fed", t))` e recebe a **mesma coisa**
(dicts). Assim a federation é **plugável no mesmo leitor** que já carrega a CLEAN — não é um
caminho paralelo. Ganhos: **um lugar** decide overwrite vs incremental (do config do conector),
trata as **três camadas + federation** igual, e os consumidores (dlt/dbt, worker, catalog-api,
run_sql) leem/escrevem pela **mesma abstração** — some a inconsistência "run_sql lê parquet e o
resto lê `.duckdb`". Trocar DuckLake↔Delta ou ADBC↔Trino vira trocar a **implementação da
classe**, não os chamadores.

- 🛑 Implementar `LakeStore` e refatorar os 4 pontos de acesso pra usá-la.
- 🛑 Backfill + cutover (ordem sem downtime) — 🔗 [migracao](migracao.md).
- 🛑 Serializar a escrita fica **menos crítico** com o catálogo Postgres (isola), mas manter o
  `concurrency limit = 1` até validar.
