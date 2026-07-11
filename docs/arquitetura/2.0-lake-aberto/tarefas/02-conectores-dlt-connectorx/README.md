# 02 · Conectores em `dlt + connectorx`

> **Sair da camada caseira** (`StratumConnector` → lista na RAM → `write_delta`) que estoura
> em 1M+ linhas, e migrar a ingestão pra **`dlt`** com backend **`connectorx`**.
>
> Base: [descobertas §1](../../descobertas.md) · benchmark:
> [folder 01](../../../../../experimentacoes/01-ingestao-fonte-para-raw/RESULTADOS.md).

Legenda: 🛑 a fazer · 🔗 detalhe noutro doc

---

## Por que

`dlt (connectorx, 8 threads)` fez **100M linhas em ~3.5min, RAM 1.4GB plana** — o caseiro é
inviável acima de 1–2M (segura a tabela inteira na RAM, sem backpressure). Ganhos que vêm de
graça com o dlt:

- `write_disposition="merge"` + `primary_key` = **upsert nativo** (mata a duplicação RAW×CLEAN).
- `dlt.sources.incremental("updated_at")` mantém o cursor → **acaba o `connector_state` manual**.

## Estado hoje

Só o `airtable` define resources dlt (e **nem usa `pipeline.run()`** —
[airtable_sync.py:236](../../../../../strattum-data/services/pipelines/src/flows/airtable_sync.py#L236)).
11/16 têm source dedicada; o resto sai pela core source REST API.

## Tarefas

- 🛑 **Migrar os flows pra `dlt.pipeline().run()`** com backend `connectorx` (bancos) —
  streaming gerenciado, sem acumular na RAM.
- 🛑 **Padronizar `write_disposition` + `primary_key`** por resource (upsert nativo).
- 🛑 **Ligar `incremental("updated_at")`** onde a fonte tem cursor (conecta com o
  levantamento por conector — [pontos-a-verificar §2](../../pontos-a-verificar.md)).
- 🛑 O **destino** (DuckLake/Delta/`.duckdb`) sai da [tarefa 01](../01-lakehouse/) — o dlt
  escreve nos três; aqui o foco é **como extrai/carrega**, não onde aterrissa.

> SaaS API (Jira, HubSpot…) segue via dlt/REST; `connectorx` é pro caminho de **banco**.
