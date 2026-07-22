# Pontos a verificar — o que ainda está aberto

> **Só o que ainda não foi decidido/validado.** O que já concluímos:
> [descobertas](descobertas.md). O que virou trabalho acionável: [tarefas/](tarefas/).
> Índice: [../README](../README.md).

As decisões maiores estão **dentro das iniciativas** (cada uma carrega sua própria decisão);
este doc guarda as investigações **transversais / de pesquisa** que não têm dono de
iniciativa. Legenda: 🛑 aberto · 🔗 vive noutro doc.

> ✅ **Saíram daqui (viraram decisão — ver [descobertas](descobertas.md)):**
> **formato do lake → DuckLake (catálogo Postgres)** (§3); **engine da federation → DuckDB +
> ADBC** (§5). O que sobrou desses temas é só **validar/medir** (maturidade do ADBC, perf), não
> mais **escolher**.

## Mapa das perguntas em aberto

> ✅ **Resolvidos pelo benchmark 2026-07-19** ([BENCHMARK-LAKEHOUSE](../../../BENCHMARK-LAKEHOUSE.md), 2M pg + 1M mongo):
> **performance do grafo** (§1 — `execute_batch` 1-MERGE/linha → `UNWIND`, **2,87×**),
> **footprint do FalkorDB** (§3 — ~510 B/elemento), **DuckLake sob volume** (2M em 37s / 1M em
> 25s, streaming, 272 MB constante), **clean dbt/DuckLake** (3M em 3,8s), **incremental
> merge-por-PK** (0 duplicatas). Seguem **abertos**: concorrência multi-conector no mesmo
> model, dedup *graph-side* (fallback `uuid4`, [tarefa 01 §C](tarefas/01-lakehouse/)),
> isolamento do container do grafo (§3.1), deep-dive por conector (§2), federation→grafo (§4).

| Pergunta | Onde vive |
|---|---|
| **Maturidade do DuckLake (1.0)** + catálogo Postgres sob concorrência | 🔗 [tarefas/01-lakehouse](tarefas/01-lakehouse/) |
| **Maturidade da extensão ADBC** (comunidade) p/ Snowflake/Databricks | 🔗 [tarefas/03-federation](tarefas/03-federation/) · §4 |
| **Incremental + duplicatas** (colunas certas na clean, do config do conector) | 🔗 [tarefas/01-lakehouse](tarefas/01-lakehouse/) |
| **`run_sql` (skills-api) só lê parquet — nem olha a CLEAN** (com DuckLake, atachar catálogo) | 🔗 [tarefas/01-lakehouse](tarefas/01-lakehouse/) · [migracao](tarefas/01-lakehouse/migracao.md) |
| **Como o `catalog-api` e o `memory-worker` leem o DuckLake** (`ATTACH ducklake:`) | 🔗 [migracao](tarefas/01-lakehouse/migracao.md) |
| **Data quality** na ingestão | 🔗 [tarefas/04-data-quality](tarefas/04-data-quality/) |
| Performance do grafo | §1 (abaixo) |
| Deep-dive por conector | §2 (abaixo) |
| Grafo em container separado | §3 (abaixo) |
| **Federation DuckDB+ADBC → grafo (validar ponta-a-ponta)** | §4 (abaixo) |

---

## §1 · Performance do grafo ✅ (medido — benchmark 2026-07-19)

**Medido** no [BENCHMARK-LAKEHOUSE](../../../BENCHMARK-LAKEHOUSE.md) (grafo de **290.756 nós +
300.000 arestas** sobre clean real, fatia de 100k contratos):

- **Throughput + onde satura:** o `execute_batch` era um **loop** — 1 `MERGE`/statement, 1
  round-trip ao FalkorDB por nó e por aresta (o "flush a cada 200" agrupava logicamente mas
  **não** reduzia as idas), a **~1.630 elem/s**. Reescrevemos pra agrupar cada lote num
  **`UNWIND $rows AS row ...`** (1 round-trip por lote de ~500) → **362s → 126s (2,87×)**,
  ~4.680 elem/s. Índices em `entity_id`/`match_field` já são criados antes do loop → `MERGE`
  é O(log n). Local, o round-trip é barato, então o custo dominante virou o `MERGE` no
  FalkorDB + a leitura do lake; num FalkorDB **remoto** o `UNWIND` rende muito mais. Teto da
  carga inicial em escala: `falkordb-bulk-loader` (CSV → grafo, milhões/s).
- **Re-`MERGE` idempotente:** confirmado — rebuilds re-MERGE sem duplicar (`entity_id`
  determinístico).
- **Cobertura:** `memory-worker/tests/test_falkordb_batch.py` (5 testes do UNWIND) +
  `test_falkordb_params.py` (9). Comparar **motores de grafo** segue fora de escopo (FalkorDB
  é a decisão).

## §2 · Deep-dive por conector 🛑

Mapear, **conector a conector** (os ~16), o comportamento real de ingestão: tem source dlt
dedicada ou cai na core REST? Tem coluna de cursor (`updated_at`/equivalente) pra incremental?
Tem `primary_key` pra upsert? Volume típico e ponto de quebra? Peculiaridades de auth/rate
limit/paginação. Alimenta [tarefas/01](tarefas/01-lakehouse/) (colunas certas) e
[tarefas/02](tarefas/02-conectores-dlt-connectorx/) (migração pra dlt).

> Formato sugerido: uma linha por conector numa tabela (conector · source dlt? · cursor ·
> PK · volume · caveats).

## §3 · Grafo em container separado 🛑 (nova investigação)

**Estado hoje** (verificado no `docker-compose.yml` do starter):

- ✅ O **FalkorDB já roda em container próprio** (`strattum-falkordb`,
  `falkordb/falkordb:v4.4.1`, 6379 interno / 6380 no host, limite de 512 MB, AOF+RDB).
- 🛑 Mas a **construção do grafo não tem container dedicado**: o `strattum-memory-worker` é
  só um **registrar one-shot** (roda `register.py` e sai); o flow de fato executa dentro do
  **`prefect-worker-ai`** genérico, que **compartilha o pool com os flows de ingestão**. O
  **DuckDB do `CleanReader` roda in-process** nesse subprocess.

**A investigar** (o que "separar o grafo" deveria resolver):

1. **Isolar a materialização do grafo** num worker pool / container dedicado, separado dos
   pipelines de dados — pra que uma ingestão pesada não dispute CPU/RAM com a carga do grafo
   (e vice-versa), e pra escalar os dois independentemente.
2. **FalkorDB dedicado por tenant / com mais recurso** — o limite de 512 MB é starter.
   ✅ **Footprint medido** ([benchmark](../../../BENCHMARK-LAKEHOUSE.md)): **~510 bytes/elemento**
   (290k nós + 300k arestas = 302 MB de RAM used / 84 MB de RDB) → o cap starter de 512 MB
   segura ~1M elementos; a projeção por RAM está no benchmark (16 GB ≈ ~19M, 64 GB ≈ ~75M,
   256 GB ≈ ~300M elementos). Segue aberto: **política de persistência** e se **multi-tenant**
   exige instância por cliente ou grafos separados na mesma instância.
3. **Leitura do lake pela rede** — com o lake aberto (2.0), o worker do grafo lê a CLEAN do
   object storage (`s3://`) em vez do arquivo local; validar latência/custo desse read
   remoto a partir de um container isolado (conecta com [tarefas/03](tarefas/03-federation/)).

> Ponto de partida no código: `docker-compose.yml` (serviços `strattum-falkordb`,
> `strattum-memory-worker`, `prefect-worker-ai`), `memory_worker/reader.py` (o `CleanReader`)
> e `memory_worker/falkordb_client.py` (cliente de rede, host/port).

## §4 · Federation DuckDB + ADBC → grafo 🛑 (validar/medir — engine já decidido)

O **engine já foi decidido: DuckDB + ADBC** ([descobertas §5](descobertas.md)). O que resta
aqui **não é escolher**, é **validar ponta-a-ponta e medir** (nenhum experimento leu o lake de
um cliente até o grafo ainda).

**Cenário:** lake/warehouse do cliente = **Databricks (Delta)** e **Snowflake** (via ADBC);
materializar no FalkorDB **sem passar pela RAW/CLEAN** (um `source` no `graph_mapping.yaml`
apontando pra fonte externa).

**O que validar/medir:**
- **Maturidade da extensão ADBC** (comunidade) — o ponto mais aberto: estabilidade, tipos,
  pushdown do filtro, drivers Snowflake/Databricks.
- **Ida ao grafo** (métrica-chave): tempo total `fonte → grafo` e **nós/aresta por segundo**.
- **Acesso a tabelas *managed* (Unity Catalog)** e credenciais de leitura do storage do cliente.
- **Custo/freshness** de reler a fonte a cada run + **dedup/idempotência** (chave estável).
- **Fallback Trino:** só medir se aparecer um caso que o DuckDB+ADBC não cubra (cliente já com
  Trino, lake distribuído muito grande).

> Onde roda: [`experimentacoes/06-federation-read-engine`](../../../experimentacoes/) — Delta em
> MinIO + Snowflake/Databricks via ADBC → DuckDB → FalkorDB (container). Alimenta
> [tarefas/03-federation](tarefas/03-federation/).
