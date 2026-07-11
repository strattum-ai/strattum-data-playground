# 07 · Federation → CLEAN via dbt — resultados

> Rodado em **DuckDB 1.4.5 · dbt-core 1.10.22 / dbt-duckdb 1.10.0 · Trino (connector
> `postgresql`) · Postgres 16**. Self-contained; tudo local.
> Notebook: [`federation_dbt.ipynb`](federation_dbt.ipynb). Projeto: [`dbt_fed/`](dbt_fed/).

## A pergunta

Dá pra o **dbt** ler **duas fontes** — uma tabela da nossa **RAW em DuckLake** e outra
da **camada federada (Trino)** —, fazer um **JOIN** e criar uma **tabela CLEAN nova de
volta na DuckLake**? O Trino aqui lê um Postgres; em produção leria Databricks/Snowflake.

```
Postgres (fed.products)  ──lido por──▶  Trino  ──┐
                                                 ├─▶  dbt-duckdb (JOIN)  ──▶  DuckLake (clean_order_lines)
DuckLake (orders = RAW)  ──attach nativo─────────┘
```

## O que aconteceu

| Etapa | Como | Resultado |
|---|---|---|
| Ler a RAW (DuckLake) | `source` dbt no banco anexado `lake` (`ATTACH 'ducklake:…'`) | 8 orders ✅ |
| Ler a fonte federada (Trino) | `source` dbt servida por um **plugin dbt-duckdb** (cliente Trino DBAPI → Arrow) | 5 products ✅ |
| JOIN das duas | modelo SQL `clean_order_lines`, **SQL puro** (engine DuckDB) | 8 linhas ✅ |
| Materializar na DuckLake | `{{ config(database='lake') }}` no modelo do join | `lake.clean_order_lines` ✅ |

Um único `dbt build` → `PASS=1` (um modelo SQL; as duas fontes são resolvidas antes). A
CLEAN final tem colunas dos **dois lados**: `qty`/`customer_email` (nossa RAW) +
`product_name`/`category`/`unit_price` (federado via Trino), com `line_total = qty ×
unit_price` no join. Tipos preservados (`unit_price` chega como `DECIMAL(6,2)`).

## Conclusão

- ✅ **Funciona.** A engine do dbt é o **DuckDB** (`dbt-duckdb`). Ele lê a DuckLake
  **nativamente** (attach) e escreve a CLEAN de volta nela — `dbt-duckdb 1.10` tem
  suporte nativo a DuckLake (flag `is_ducklake`, detectada pelo scheme `ducklake:`).
- **O Trino entra por uma ponte, não nativamente.** O DuckDB **não tem connector Trino**
  — a fonte federada é servida por um **plugin dbt-duckdb** ([`plugins/trino_source.py`](dbt_fed/plugins/trino_source.py))
  que usa o **cliente Trino (DBAPI)**: `SELECT` no Trino → Arrow → dbt registra a tabela.
  É a alternativa direta ao **ODBC/JDBC**; em Python o DBAPI é o mais simples.
- **Máxima flexibilidade = plugin + `source` declarativa.** Uma instância do plugin serve
  **qualquer** tabela de **qualquer** catálogo do Trino. Adicionar fonte federada nova =
  **umas linhas de YAML** em `sources.yml`, **zero Python**. O schema Arrow é **inferido**
  (sem colunas/tipos hardcoded, sem `CAST`), e o JOIN é **SQL puro**. Três modos por
  tabela: `meta.query` (SQL completo → pushdown), `meta.relation`, ou `meta.catalog`.
- **`database='lake'` roteia a escrita.** Setar `database` no `config()` do modelo faz o
  `CREATE TABLE` cair no catálogo DuckLake anexado, em vez do `.duckdb` "engine".
- **Trocar Postgres por Databricks/Snowflake é só config**: muda o `relation`/`catalog` no
  YAML (`postgresql` → `delta`/`snowflake`). O join e a escrita na DuckLake não mudam.

## Caveats (a cravar)

- **Sem incremental / pushdown automático** neste teste: por padrão a federação **relê a
  fonte inteira** a cada `dbt build` e o dado do Trino passa **pela RAM** do processo do
  dbt (o plugin roda in-process). Mitigação: `meta.query` empurra filtro/agg pro Trino; e/
  ou materialização incremental.
- **Federação é unidirecional:** o Trino traz o lado do cliente (Delta/Iceberg/Snowflake),
  mas **não lê DuckLake** (mesmo achado do [`06`](../06-federation-read-engine/RESULTADOS.md)).
  A escrita final é sempre **DuckDB → DuckLake**.
- **`threads: 1`** — DuckLake é single-writer e o attach vive numa conexão compartilhada.
- **`PYTHONPATH`** precisa incluir o dir do projeto pro `module: plugins.trino_source`
  importar (o notebook exporta isso).
- Testado com Trino lendo **Postgres local** (`host.docker.internal`). Contra Databricks/
  Snowflake real é troca de catálogo — falta rodar (mesmo caveat do 05/06).
- **Alternativa mais simples (menos flexível):** um **modelo Python** (`.py` em `models/`)
  fazendo o `SELECT` no Trino direto. Resolve 1 tabela, exige 1 arquivo por fonte e (se o
  schema for escrito à mão) vira rígido. Bom pra caso pontual; o plugin escala melhor.

## Relação com o folder 06

`06` provou **federação → grafo** (leitura direta do lake do cliente alimentando o
FalkorDB, sem ETL). `07` prova o **caminho de escrita**: federação + RAW local → **CLEAN
nova via dbt**. Complementares — juntos cobrem os dois usos da federação no diagrama 2.0.

> Alimenta a decisão *engine da federation (DuckDB vs Trino)*:
> [pontos-a-verificar §4 / tarefas/03-federation](../../docs/arquitetura/2.0-lake-aberto/pontos-a-verificar.md).
