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

## §1 · Performance do grafo 🛑

Ainda **a medir**. O caminho CLEAN→grafo já é em batch (flush a cada 200 statements via
`execute_batch`), mas nunca medimos throughput/latência de carga sob volume real. A investigar:

- Throughput de `MERGE` no FalkorDB (nós + arestas) por segundo, com/sem índices em
  `entity_id` e nos `match_field` das arestas.
- Onde satura: driver `falkordb` (protocolo Redis), tamanho do batch, ER em Python.
- Custo de re-`MERGE` idempotente vs `CREATE` na primeira carga.
- Comparar motores de grafo **fica fora de escopo por ora** — FalkorDB é a decisão atual.

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
2. **FalkorDB dedicado por tenant / com mais recurso** — o limite de 512 MB é starter; medir
   footprint sob volume real, política de persistência, e se multi-tenant exige uma instância
   por cliente ou grafos separados na mesma instância.
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
