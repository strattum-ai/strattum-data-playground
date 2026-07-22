# Tarefas — iniciativas da arquitetura 2.0

> **Backlog acionável, agrupado por iniciativa** (uma pasta cada). Deriva das
> [descobertas](../descobertas.md) e responde aos [pontos-a-verificar](../pontos-a-verificar.md).
> Cada pasta traz: objetivo, decisão a tomar (se houver), checklist de tarefas, dependências.
> Índice geral: [../../README](../../README.md).

| # | Iniciativa | O que entrega | Prioridade |
|---|---|---|---|
| [01](01-lakehouse/) | **Lakehouse** — trocar o `.duckdb` por lake aberto (DuckLake **ou** Delta) e, no mesmo movimento, **incremental + fim das duplicatas** (colunas certas na clean) | storage aberto + snapshot isolation + sync incremental correto | 🥇 alta |
| [02](02-conectores-dlt-connectorx/) | **Conectores em `dlt + connectorx`** — sair da camada caseira que estoura RAM | ingestão que escala a 100M sem OOM | 🥇 alta |
| [03](03-federation/) | **Federation** — ler o lake do cliente (Databricks/Snowflake) sem ETL; decidir **Trino vs DuckDB**; tratar o catálogo | atender empresa que já tem lake | 🥈 média (Snowflake: baixa) |
| [04](04-data-quality/) | **Data quality** — gate de qualidade na ingestão (staging → RAW \| quarentena) | dado ruim/bot não vaza pro grafo | 🥉 depois |
| [05](05-acl-por-objeto/) | **ACL por objeto** — extrair a autorização das fontes (BigQuery, Confluence, HubSpot, Jira, Salesforce) e persistir no Postgres: qual objeto extraído, quem tem acesso | recuperação **permission-aware** (não vaza dado que o usuário não veria na fonte) | 🔥 urgente |

## Dependências

- **01 e 02 são a base** e podem andar em paralelo. A **01 define o formato do lake**, e a
  **02 escreve nele** (o `dlt` grava nos três — a escolha do destino sai da 01).
- **03 (federation)** assume o lake aberto da 01 (mesmo `delta_scan`/`ducklake attach`) mas
  não depende do backfill — lê o lake **do cliente**.
- **04 (data quality)** entra depois que a ingestão 02 estabilizar (o gate fica antes do RAW).
- **05 (ACL por objeto)** anda junto da 02 — a autorização é extraída no **mesmo run** do
  conector. É **urgente** porque bloqueia servir dado a cliente enterprise sem vazar o que o
  usuário não veria na fonte (recuperação permission-aware).
