# 08 · Federation via ADBC — resultados

> Rodado em **DuckDB 1.4.5 · extensão `adbc` (community, query-farm) · adbc-driver-postgresql 1.8.0**.
> Fonte = Postgres (papel do Databricks/Snowflake). Script: [`federate_adbc.py`](federate_adbc.py).

## A pergunta

Saiu a [extensão ADBC do DuckDB](https://columnar.tech/blog/announcing-duckdb-adbc-extension/).
Com ela dá pra o DuckDB conectar num warehouse (Databricks/Snowflake) **direto** e, no dbt,
juntar a **nossa RAW (DuckLake)** com a fonte federada — **sem Trino e sem plugin custom**?

## O que aconteceu

| Etapa | Como | Resultado |
|---|---|---|
| Instalar a extensão | `INSTALL adbc FROM community; LOAD adbc;` (duckdb 1.4.5) | expõe `read_adbc`, `adbc_execute` ✅ |
| Registrar o driver | manifesto `postgresql.toml` + `ADBC_DRIVER_PATH` (scheme da URI = nome do driver) | ✅ |
| Ler a fonte externa | `read_adbc('postgresql://…','SELECT … FROM fed.products')` | 5 products, **Arrow**, direto ✅ |
| JOIN com a DuckLake | `lake.orders JOIN read_adbc(...)` em SQL puro | 5 linhas ✅ |

`read_adbc(uri, query)` é uma **table function** — dá pra usar num modelo/`source` do
dbt-duckdb. **Zero Trino, zero plugin, zero Python de cola.**

## Conclusão

- ✅ **Funciona** e é a via mais enxuta pro caso DuckLake: a engine continua **DuckDB**
  (então lê/escreve DuckLake), mas a federação vira **nativa** via ADBC — transferência
  **Arrow zero-copy**, **sem cluster Trino** e **sem o plugin** do [`07`](../07-federation-dbt-clean/).
- **Databricks é suportado** (o anúncio lista, e existe um driver ADBC dedicado — v0.1.2,
  jan/2026, adbc-drivers.org, via Thrift+Cloud Fetch+Arrow). No dbt: adicionar `adbc` às
  extensions, apontar o manifesto pro driver Databricks, e a `source` vira
  `read_adbc('databricks://…', 'SELECT … FROM catalog.schema.tabela')`.
- **Resolução de driver:** o **scheme da URI** (`postgresql`, `databricks`) vira o nome do
  driver, achado por um manifesto `<nome>.toml` em `ADBC_DRIVER_PATH`.

## Caveats (a cravar)

- **Driver Databricks é 0.1.x** — provamos com Postgres; Databricks especificamente exige
  validar maturidade/performance. Não existe wheel `adbc-driver-databricks` no PyPI (o
  driver vem do foundry adbc-drivers.org como lib compilada). Snowflake/BigQuery/FlightSQL
  têm wheel oficial (arrow-adbc 1.8).
- **Pushdown é manual:** `read_adbc(uri, query)` empurra pro warehouse o que você escreve
  na query (filtro/agg). O modo `ATTACH` **não** tem pushdown (varre tudo pra RAM).
- **Single-node:** o JOIN roda no DuckDB, um processo; o resultado federado passa pela RAM.
  Pra scan gigante, o motor distribuído do Trino ainda ganha no scan.
- **Tipos dependem do driver:** o driver Postgres devolveu `numeric` como **texto** →
  precisou `CAST`. Cada driver tem seu mapeamento.
- Extensão **community**, autocommit-only, sem transações multi-statement. Nova.

## Onde entra na decisão

Terceira via de *engine de federação*, e a mais coerente quando **o lake é DuckLake**:
mantém DuckDB (que o Trino não substitui pra DuckLake — ver [`06`](../06-federation-read-engine/RESULTADOS.md))
e ainda assim federa Databricks/Snowflake de forma nativa e declarativa.
