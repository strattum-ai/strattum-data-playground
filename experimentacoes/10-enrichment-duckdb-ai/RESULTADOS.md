# 10 · Enrichment com `duckdb-ai` — resultados

> **Status: 🛑 A RODAR.** Este doc é o template — preencher depois de rodar
> `enrichment_duckdb_ai.sql` (caminho A) e `run_enrichment.py` (caminho B). Alimenta a
> **decisão da [tarefa 01 §enrichment](../../documents/arquitetura/2.0-lake-aberto/tarefas/01-lakehouse/README.md)**:
> adotar `duckdb-ai` na camada enrichment (uma engine só) ou manter o estágio Python do
> [`ai_enrichment_pipeline`](../../../strattum-data/services/pipelines/src/flows/ai_enrichment_pipeline.py).

Extensão: [`leonardovida/duckdb-ai`](https://github.com/leonardovida/duckdb-ai) · versão testada: **v0.4.7** ·
provider: **Anthropic** (`claude-haiku-4-5`).

## A hipótese

`RAW → enrichment (LLM) → CLEAN` vira **um model dbt** (`SELECT ai_extract_record(...) FROM raw`)
— DuckDB faz ingestão, enrichment, transform e federation. Adeus estágio Python separado.

## O que rodou

| Query | Resultado |
|---|---|
| `ai_extract_record` (4 transcripts → STRUCT tipado) | _(a preencher)_ |
| CLEAN achatada (`clean_calls`) | _(a preencher — colar as 4 linhas)_ |
| `ai_usage()` (tokens / custo / retry / cache) | _(a preencher)_ |
| Caminho B: escreve `lake.enrichment.calls` + `lake.clean.calls` na DuckLake | _(a preencher — ✅/🛑)_ |

## Checklist de paridade com o `ai_enrichment` atual

> É isto que decide se adotamos. Marcar ✅/🛑 por linha depois de rodar.

| # | Requisito do pipeline atual | Como o `duckdb-ai` cobre | Status |
|---|---|---|---|
| 1 | **Enrichment como SQL/dbt** (uma engine só) | `ai_extract_record` num `SELECT` → model dbt | 🛑 |
| 2 | **Structured output estável** (transcript → colunas) | `ai_extract_record(text, json_schema)` → STRUCT tipado (schema constante) | 🛑 |
| 3 | **Cache / idempotência** (re-run barato, não recobra) | `SET duckdb_ai_cache=true` + `cache_ttl_seconds`; cache hit no `ai_usage()` | 🛑 |
| 4 | **Prompt caching** (Anthropic) | `SET duckdb_ai_prompt_cache=true` | 🛑 |
| 5 | **Retry / backoff / dead-letter** | retry_count no `ai_usage()`; `ai_try_complete` p/ capturar erro por linha | 🛑 |
| 6 | **Budget de custo** por run | `ai_usage()` (tokens + custo estimado) + `ai_model_prices()` | 🛑 |
| 7 | **Concorrência / rate-limit** | `max_concurrent_requests` (0–64) + `ai_recommended_batch_size` | 🛑 |
| 8 | **Escreve na DuckLake** (camada enrichment) | caminho B: `CREATE TABLE lake.enrichment.calls AS SELECT ai_...` | 🛑 |
| 9 | **Provider Anthropic** (mesmo já usado) | `AI_PROVIDER 'anthropic'` via secret | 🛑 |
| 10 | **Dependência dura** (clean só após enrichment) | orquestração dbt (`ref()`), fora do escopo do teste SQL — anotar | 🛑 |
| 11 | **Maturidade** (projeto novo, v0.4.x) | anotar versão + bugs encontrados | 🛑 |

## Gaps / o que o `duckdb-ai` NÃO cobre (preencher)

- _(ex.: cache por versão de rubrica/código? retry configurável? dead-letter table nativa?)_

## Recomendação

_(a preencher: **adotar** no enrichment / **adotar parcial** (só extract, orquestração no dbt) /
**manter Python** por causa de X. Se adotar, próximo passo = protótipo de model dbt em
[tarefa 01](../../documents/arquitetura/2.0-lake-aberto/tarefas/01-lakehouse/README.md).)_
