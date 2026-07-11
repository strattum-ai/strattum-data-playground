# Migração para o lake aberto — código + runbook

> **Plano de migração do `.duckdb` para o lakehouse aberto.** Storage decidido: **DuckLake**
> (catálogo Postgres) — o **Delta** fica como fallback e aparece marcado **[Delta]** onde
> diverge. O acesso ao lake é centralizado na classe **`LakeStore`** ([tarefa 01 §E](./)) —
> este doc é o runbook de código/dados por trás dela. O caminho até o grafo **não muda** —
> FalkorDB só fala Cypher sobre linhas; a migração toca **escrita e leitura das camadas**.
>
> Base: [folder 05](../../../../../experimentacoes/05-formato-storage-lake/RESULTADOS.md). Índice:
> [../README](../../README.md). Legenda: ✅ pronto/validado · 🛑 a fazer · ⚠️ cuidado

---

## O que é o "lake" aqui (sem Spark, sem JVM)

Tudo **in-process, single-node** — combina com o DuckDB embarcado + Prefect na VM:

- **[Delta]** = **delta-rs** (Rust). Escrever: `deltalake.write_deltalake(...)` / `DeltaTable.merge`
  (o `write_delta` da plataforma **já usa** — [connectors/utils/delta.py](../../../../../strattum-data/services/pipelines/src/connectors/utils/delta.py)).
  Ler: extensão `delta` do DuckDB (`delta_scan`). Uma tabela = uma pasta (`_delta_log/` + parquet).
- **[DuckLake]** = catálogo SQL (DuckDB/Postgres) + parquet no object storage. Escrever/ler:
  `ATTACH 'ducklake:<catálogo>'` e SQL normal — o dbt-duckdb materializa **nativo**.

> A plataforma **já escreve Delta** (na RAW) e **já lê parquet via glob** (fallback do
> `CleanReader`). A migração da CLEAN reusa essas peças — não entra tecnologia nova.

## O que migra

O lake tem **três camadas** — `raw`, `enrichment` (LLM, [descobertas §6](../../descobertas.md)) e
`clean` — todas passam a viver no **DuckLake**. A CLEAN é a que sai do `.duckdb` (schema
`main_clean`); RAW já é aberta e o enrichment é novo:

```
strattum.duckdb::main_clean.dim_companies  ->  lake.clean.dim_companies      (catálogo DuckLake)
strattum.duckdb::main_clean.fct_tickets     ->  lake.clean.fct_tickets        (catálogo DuckLake)
        (raw e enrichment: idem — lake.raw.*, lake.enrichment.*)
```

> Todas as leituras/escritas passam pela classe **`LakeStore(catalog, storage)`** que atacha o
> catálogo uma vez e expõe `read(layer, table, …)` / `write(layer, table, arrow, mode=…)` —
> ver o desenho em [tarefa 01 §E](./).

> 💡 **Mesmo movimento, o storage sai do volume Docker (`strattum_data_local:/data`) pro
> MinIO (`s3://`).** O destino `/data/clean/...` abaixo vira `s3://strattum-lake/clean/...`
> — o backfill e os leitores (via `httpfs`) já aceitam `s3://`. Ver
> [tarefa 01 §D](./).

---

# Parte 1 — Onde mudar o código

Cinco pontos. Os três de **leitura** (catalog-api, memory-worker, skills-api) são o "contrato" da CLEAN.

## Ponto 1 — Ingestão (RAW) — ✅ já é aberta

A RAW já é Delta. O `dlt` escreve nos dois formatos nativamente (`table_format="delta"` ou
destination `ducklake`) — nada obrigatório aqui.

## Ponto 2 — Escrita da CLEAN (dbt) — 🛑 diverge por formato

**Arquivo:** `strattum-data/services/pipelines/dbt/` +
[`flows/utils.py`](../../../../../strattum-data/services/pipelines/src/flows/utils.py) (`_run_dbt`).

- **[DuckLake] ✅ nativo.** `dbt-duckdb` dá `attach` no catálogo e materializa direto no lake
  (`--full-refresh` = overwrite; `run` = incremental). **Sem ponte, sem passo extra.**
- **[Delta] 🛑 o `dbt-duckdb` não escreve Delta — só lê.** Duas saídas (folder 05 §3–§4):
  1. **Ponte pós-dbt:** dbt materializa numa tabela scratch → passo Python
     `write_deltalake`/`merge` por `primary_key`. Encapsular num helper no `_run_dbt`.
  2. **Plugin `store()`** ([`delta_writer.py`](../../../../../experimentacoes/05-formato-storage-lake/dbt/delta_writer.py)):
     um `dbt run` escreve Delta (overwrite + merge), sem passo externo. ⚠️ Ambas têm **write
     duplo** (parquet temp → Delta) — inevitável (não há `COPY TO (FORMAT delta)` no DuckDB).

## Ponto 3 — Leitura: `memory-worker` (CLEAN → grafo) — 🛑 novo backend no `CleanReader`

O `CleanReader` **já é abstraído por backend** — hoje decide entre **arquivo `.duckdb`** e
**glob de parquet** ([reader.py:68-96](../../../../../strattum-ai/services/memory-worker/memory_worker/reader.py#L68)),
e já carrega `httpfs` pra `s3://`. Adicionar o backend do lake é localizado:

```python
# hoje (reader.py:214-248, _build_select) — backend .duckdb:
sql = f'SELECT {col_list} FROM main_clean."{table}"'
# [Delta]  -> delta_scan (+ INSTALL delta; LOAD delta; junto do httpfs em :95-96)
sql = f"SELECT {col_list} FROM delta_scan('{self._storage_root}/clean/{table}')"
# [DuckLake] -> ATTACH 'ducklake:<catálogo>' e SELECT do schema anexado
```

O filtro de watermark (`WHERE {updated_at} > ...`, [:237-246](../../../../../strattum-ai/services/memory-worker/memory_worker/reader.py#L237)),
o `iter_rows` (batch 1000), a geração de Cypher e a escrita no FalkorDB **ficam idênticos** —
tudo continua recebendo dicts.

## Ponto 4 — Leitura: `catalog-api` — 🛑 trocar a conexão `.duckdb` pelo scan do lake

Hoje o catalog-api abre o **arquivo `.duckdb` read-only** e consulta o schema `main_clean`
([catalog.py:33-39](../../../../../strattum-data/services/catalog-api/src/routers/catalog.py#L33)):

```python
_duckdb_path() -> os.environ.get("DBT_DUCKDB_PATH", "/data/strattum.duckdb")
duckdb.connect(_duckdb_path(), read_only=True)   # schema hardcoded: main_clean
```

Três leituras a migrar ([catalog.py:42-134](../../../../../strattum-data/services/catalog-api/src/routers/catalog.py#L42)):

| Função | Hoje | Depois |
|---|---|---|
| `list_clean_tables()` | `duckdb_tables()` no schema `main_clean` | **[Delta]** listar pastas em `/data/clean/*` · **[DuckLake]** `SHOW TABLES` no catálogo anexado |
| `get_clean_row_count()` | `SELECT COUNT(*) FROM main_clean."{t}"` | `... FROM delta_scan('/data/clean/{t}')` · ou catálogo DuckLake |
| `read_clean_preview()` | ILIKE + sort + paginação em `main_clean."{t}"` | mesma query trocando a fonte pelo scan/attach |

> ⚠️ **[Delta] sem catálogo, listar tabelas vira convenção de paths** (`/data/clean/<t>/`).
> O `.duckdb` dava `list_clean_tables` de graça (`information_schema`). **[DuckLake]** mantém
> o catálogo único — `list_clean_tables` continua trivial. É um argumento a favor do DuckLake
> nesse ponto (ver [tarefa 01](./)).

## Ponto 5 — Leitura: `skills-api` (o `run_sql` do MCP) — 🛑 terceiro leitor da CLEAN

O **MCP não muda**: `run_sql`/`run_cypher`/`get_schema` são adaptadores finos que **não conectam
em banco** — fazem POST pra skills-api
([mcp-server/tools/generic.py:8,95](../../../../../strattum-ai/services/mcp-server/src/mcp_server/tools/generic.py#L95)).
Quem roda o SQL é a **skills-api** (`POST /v1/skills/sql`): abre **DuckDB in-memory** e registra
como views os **parquet** de `/data/clean` e `/data/semantic`
([query.py:202-240](../../../../../strattum-ai/services/skills-api/src/skills_api/routers/query.py#L202)):

```python
duckdb.connect(":memory:")
# glob /data/clean/**/*.parquet e /data/semantic/**/*.parquet -> CREATE VIEW ... read_parquet(...)
```

- ⚠️ **Já hoje lê parquet** (`read_parquet` em `/data/clean`), **não** o `strattum.duckdb`. Como a
  CLEAN atual mora no `.duckdb`, **confirmar se `run_sql` enxerga a clean hoje** (se não há
  parquet exportado em `/data/clean`, o `run_sql` não vê a clean — bug latente).
- **[Delta]** trocar o glob por `delta_scan('/data/clean/{t}')` (+ `INSTALL delta`); `DUCKDB_DATA_PATH`
  aponta o lake (hoje `/data`).
- **[DuckLake]** `ATTACH 'ducklake:<catálogo>'` e registrar as tabelas do catálogo.
- **Argumento a favor do lake aberto:** com Delta/DuckLake, os três leitores (grafo, catalog-api,
  `run_sql`) passam a ler **a mesma fonte** — hoje o `run_sql` lê parquet e os outros o `.duckdb`,
  o que já é inconsistente.

## O que NÃO muda

- ✅ `cypher_generator.py`, `falkordb_client.py`, o loop do `pipeline.py` (ER, watermark, batch).
- ✅ `graph_mapping.yaml` (as fontes continuam `clean/<tabela>`).
- ✅ Prefect (flows/tasks), o gatilho `_trigger_memory_worker` após o dbt.
- ✅ **FalkorDB** — já é container próprio; a materialização do grafo é discussão à parte
  ([pontos-a-verificar §3](../../pontos-a-verificar.md)).

---

# Parte 2 — Como rodar a migração (dados)

## Passo 1 — Backfill (`.duckdb` → lake) — ✅ script testado

[`migrate_duckdb_to_lake.py`](../../../../../experimentacoes/05-formato-storage-lake/migrate_duckdb_to_lake.py)
reescreve cada tabela do `.duckdb` no lake. Idempotente (`overwrite`) e **read-only** no
`.duckdb` (não corrompe a saída do dbt). Verifica que a contagem bate no fim.

```bash
# [Delta] local ou s3:// (AWS_* no ambiente)
python migrate_duckdb_to_lake.py --duckdb /data/strattum.duckdb --schema main_clean --out /data/clean
python migrate_duckdb_to_lake.py --duckdb /data/strattum.duckdb --schema main_clean --out s3://strattum-lake/clean
# [DuckLake] backfill análogo: ATTACH do catálogo + INSERT ... SELECT por tabela (sem write_deltalake)
```

## Passo 2 — Ordem segura (sem downtime)

```
1. backfill (.duckdb -> lake)          # dados nos dois lugares
2. pipeline escreve OS DOIS            # .duckdb + lake em paralelo (dual-write temporário)
3. cutover das leituras -> lake        # memory-worker (P3) + catalog-api (P4) + skills-api/run_sql (P5)
4. validar (contagens, nós no grafo)
5. desligar a escrita do .duckdb + backup do arquivo
```

- **Rollback:** enquanto o `.duckdb` existir e for escrito (passo 2), voltar é só reapontar a
  leitura. Guarde o `strattum.duckdb` como backup até validar em produção.
- ⚠️ **Arquivos pequenos [Delta/DuckLake]:** muitos merges incrementais geram muitos parquet —
  agende `OPTIMIZE`/compaction + `VACUUM` (delta-rs: `DeltaTable(...).optimize.compact()` / `.vacuum()`).

## Passo 3 — Validar e virar a chave

1. Rodar o memory-worker apontando pro lake e conferir a contagem de nós no FalkorDB (o
   folder 05 §3 provou o caminho lake→grafo).
2. Comparar CLEAN lake vs `.duckdb` (o próprio script de backfill faz isso).
3. Só então **parar de escrever o `.duckdb`**.
