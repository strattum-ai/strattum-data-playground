# Pontos a verificar — o que ainda está aberto

> **Só o que ainda não foi decidido/validado.** O que já concluímos:
> [descobertas](descobertas.md). O que virou trabalho acionável: [tarefas/](tarefas/).
> Índice: [../README](../README.md).

As decisões maiores estão **dentro das iniciativas** (cada uma carrega sua própria decisão);
este doc guarda as investigações **transversais / de pesquisa** que não têm dono de
iniciativa. Legenda: 🛑 aberto · 🔗 vive noutro doc.

## Mapa das perguntas em aberto

| Pergunta | Onde vive |
|---|---|
| **DuckLake vs Delta** (formato do lake) | 🔗 [tarefas/01-lakehouse](tarefas/01-lakehouse/) |
| **Incremental + duplicatas** (colunas certas na clean) | 🔗 [tarefas/01-lakehouse](tarefas/01-lakehouse/) |
| **Trino vs DuckDB** (engine da federation) + **catálogo** | 🔗 [tarefas/03-federation](tarefas/03-federation/) |
| **Data quality** na ingestão | 🔗 [tarefas/04-data-quality](tarefas/04-data-quality/) |
| **Como o `catalog-api` lê os dois formatos** | 🔗 [migracao §catalog-api](migracao.md) |
| **Como o `memory-worker` muda** (o `CleanReader`) | 🔗 [migracao §memory-worker](migracao.md) |
| Performance do grafo | §1 (abaixo) |
| Deep-dive por conector | §2 (abaixo) |
| Grafo em container separado | §3 (abaixo) |
| **Ler o Databricks: Trino vs DuckDB → grafo** | §4 (abaixo) |

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

## §4 · Ler o Databricks: Trino vs DuckDB → grafo 🛑 (experimentação)

A federation ainda **não é validada** (nenhum experimento leu o lake de um cliente ponta a
ponta até o grafo — 🔗 [tarefas/03-federation](tarefas/03-federation/)). Esta experimentação
valida **e compara** os dois engines de leitura, medindo a **ida ao grafo**.

**Cenário:** lake do cliente = **Databricks (Delta)**; materializar no FalkorDB **sem passar
pela RAW/CLEAN** (um `source` no `graph_mapping.yaml` apontando pro lake).

**Dois caminhos:**

| | **DuckDB** | **Trino** |
|---|---|---|
| Como lê o Databricks | `delta_scan('s3://…')` ou `uc_catalog` (Unity) | conector Delta/Databricks |
| Infra | **in-process** (o `CleanReader` já é DuckDB) — zero infra nova | **cluster** (coordinator + workers) |
| Caminho até o grafo | scan → dict → ER → Cypher `MERGE` → FalkorDB | Trino → worker (`source: trino/<catalog>…`) → mesmo resto |

**O que medir/comparar:**
- **Ida ao grafo** (métrica-chave): tempo total `lake → grafo` e **nós/aresta por segundo**, nos dois.
- **Planning + pushdown** do filtro incremental (`WHERE updated_at > watermark`).
- **Acesso a tabelas *managed* (Unity Catalog)** — o ponto duro; testar nos dois.
- **Custo/freshness** de reler o lake a cada run.
- **Esforço de wiring** no `graph_mapping.yaml` (source `delta_scan`/`uc_catalog` vs catalog Trino).
- **Resultado igual** no grafo (contagem de nós/arestas) + idempotência (chave estável).

**Hipótese:** pra Databricks (que é Delta) o **DuckDB lê direto** e é o caminho leve; o **Trino**
compensa quando entra **multi-fonte**, **Snowflake junto** (DuckDB não lê Snowflake nativo) ou
**tabelas grandes** que pedem distribuído.

> Onde roda: [`experimentacoes/06-federation-read-engine`](../../../experimentacoes/) — Delta em
> MinIO simulando o lake do cliente → os dois engines → FalkorDB (container). Alimenta a decisão
> **Trino vs DuckDB** de [tarefas/03-federation](tarefas/03-federation/).
