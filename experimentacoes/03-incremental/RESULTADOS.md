# 03 · Incremental — resultados e o que mudar

Vale a pena ir incremental, e o que mudar no projeto? Notebook: `05_incremental.ipynb` (1M linhas).

## Onde estamos hoje

| Estágio | Hoje | Situação |
|---|---|---|
| **source → RAW** | cursor em `connector_state` + merge por PK | ✅ funciona |
| **RAW → CLEAN** | 42 models `materialized='table'` | 🛑 full-refresh sempre |
| **CLEAN → grafo** | watermark `updated_at`; sem a coluna → full scan | 🛑 quebra em 24/42 |

## O que medimos (1M)

| | tempo | nota |
|---|---|---|
| RAW→CLEAN **full** | ~160 ms | reprocessa tudo |
| RAW→CLEAN **incremental** (delta 20k) | ~20 ms | **8×** mais rápido a 2% de delta |
| **skip** (nada mudou) | ~0,2 ms | se delta = 0, pula o dbt |

Ganho cresce com a tabela (delta fixo, tabela cresce). Vale a pena. *(Tempos baixos
porque o modelo é leve — importa a razão full × incremental, não o absoluto.)*

## O bug é no CLEAN → grafo (não no RAW)

- **Gatilho:** clean sem `updated_at` → `CleanReader` ignora o watermark e faz full
  scan ([reader.py:232](../../strattum-ai/services/memory-worker/memory_worker/reader.py#L232)). Atinge 24/42.
- **Dano:** o upsert no grafo não é idempotente em alguns casos (`entity_id`
  não-determinístico, eventos com `CREATE`) → **duplica** no FalkorDB.

São **duas correções separadas**: `updated_at` na clean (barata) e idempotência do
grafo (memory_worker). O "tudo DuckDB" não resolve nenhuma — é coluna + idempotência.

## O que mudar pra ficar tudo incremental

Base de tudo: cada clean model precisa de **`external_id`** (chave do merge, ✅ 42/42)
e **`updated_at`** (filtro do delta + watermark do grafo, 🛑 só 18/42).

| # | Mudança | Onde |
|---|---|---|
| **1** 🔑 | `updated_at` + `external_id` nas 24 clean models que faltam | `connectors/*/transforms/*.sql` |
| **2** | `materialized='table'` → `incremental` (merge) + `is_incremental()` | mesmos `.sql` |
| **3** | `primary_key` no `TableConfig` (hoje hardcoded em 4 flows) | `connectors/*/config.py` |
| **4** | Skip o dbt se `load_info` do dlt = 0 linhas | flow |
| **5** | source→RAW via dlt (`incremental` + `merge` nativos) | ingestão ("tudo DuckDB") |

Não se escreve `MERGE` na mão — o dbt gera:

```sql
{{ config(materialized='incremental', unique_key='external_id', incremental_strategy='merge') }}
SELECT ..., updated_at FROM {{ source('raw','x') }}
{% if is_incremental() %} WHERE updated_at > (SELECT max(updated_at) FROM {{ this }}) {% endif %}
```

## Decisão

- **Sim, vale ir incremental no RAW→CLEAN.** Comece pelo **item 1** (`updated_at` na
  clean) — resolve o bug do grafo **e** destrava o incremental.
- **24 sem `updated_at`:** projetar a coluna se a fonte tiver timestamp (maioria tem);
  senão, manter full ou usar hash de conteúdo. Não inventar `updated_at` falso.
- **Ressalva:** window (`rank`) + incremental não combinam — a clean real não usa window.
