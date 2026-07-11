# 03 · Federation — ler o lake do cliente sem ETL

> Para a empresa que **já tem lake** (Databricks/Snowflake), não faz sentido copiar tudo pra
> nossa RAW/CLEAN. Uma engine lê o lake dele **direto → grafo**, sem ETL. O grafo não sabe a
> origem — só recebe dicts (mesmo loop do `memory-worker`).
>
> Base: [descobertas §5](../../descobertas.md) · experimento:
> [folder 06](../../../../../experimentacoes/06-federation-read-engine/RESULTADOS.md).

Legenda: 🛑 a fazer · ⚖️ decisão · 🔗 detalhe noutro doc

---

## ⚖️ Decisão a tomar: Trino vs DuckDB

| Engine | Delta | DuckLake | Iceberg/Snowflake | Infra |
|---|---|---|---|---|
| **DuckDB** | ✅ `delta_scan` | ✅ `attach` | Iceberg ✅ · Snowflake via extensão experimental | in-process (é o que o memory-worker já usa) |
| **Trino** | ✅ | 🛑 **sem connector** | ✅ connectors nativos | cluster + catálogo/metastore |

- **DuckDB** é a mais flexível e encaixa direto (só troca o `_build_select` do reader).
- **Trino** serve quando o cliente já tem Trino / lake distribuído, ou pra Snowflake nativo.
- 🛑 **Regra dura:** cliente em **DuckLake ⇒ DuckDB obrigatório** (Trino não lê DuckLake).

## Tarefas

- 🛑 **Validar ponta-a-ponta contra lake real em S3** (Databricks/Delta primeiro) → grafo,
  sem RAW/CLEAN. Hoje só rodou em disco local.
- 🛑 **Resolver dedup / freshness / custo** da releitura (federação relê a fonte toda vez).
- 🛑 **Databricks/Delta** é a prioridade. **Snowflake é baixa prioridade** — avaliar via
  Trino (connector nativo) vs DuckDB (extensão experimental).
- 🛑 **Como lidar com o catálogo?** — descoberta de tabelas do lake do cliente:
  - DuckDB: `delta_scan`/`iceberg_scan` por path, ou attach de catálogo externo.
  - Trino: catalog + metastore (Hive/Glue/Unity).
  - **ACL / Unity Catalog *managed*** exige credencial de leitura do storage do cliente
    (suporte a *managed* no DuckDB é novo) — mapear o que dá e o que não dá ler.
- 🔗 Como o `catalog-api` e o `memory-worker` passam a apontar pro lake: [migracao](../../migracao.md).
