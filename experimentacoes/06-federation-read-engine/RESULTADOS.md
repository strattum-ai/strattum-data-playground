# 06 · Federation (no-ETL) — read-engine: DuckDB vs Trino — resultados

> Rodado em **DuckDB 1.5.4 · deltalake 1.6.1 · Trino (connector delta_lake) · FalkorDB**.
> Self-contained; CLEAN do cliente simulada em disco (Delta + DuckLake).
> Scripts: [`federate_duckdb.py`](federate_duckdb.py), [`federate_trino.py`](federate_trino.py).
> Notebook: [`federation.ipynb`](federation.ipynb).

## A pergunta

Path **FEDERATION (no-ETL)** do diagrama "Futuro": se a CLEAN é **do cliente** e ele não
quer ETL, dá pra uma engine (DuckDB **ou** Trino) **ler direto** o lake dele (Delta **ou**
DuckLake) e alimentar o FalkorDB, **sem copiar nada** pra nossa RAW/CLEAN?

## O que aconteceu

| Engine | Formato do cliente | Leitura direta | Nós no FalkorDB |
|---|---|---|---|
| **DuckDB** | Delta | `delta_scan('…/orders')` | 50 ✅ |
| **DuckDB** | DuckLake | `ATTACH 'ducklake:…' (READ_ONLY)` | 50 ✅ |
| **Trino** | Delta | connector `delta_lake` + `register_table` | 50 ✅ |
| **Trino** | DuckLake | — | ❌ **sem connector** |

Em todos os que passaram: `engine lê o lake do cliente → linhas em RAM → 1 MERGE por
linha no FalkorDB` — idêntico ao loop do `memory_worker`. **Zero escrita** na nossa RAW/CLEAN.

## Conclusão

- ✅ **O "No ETL" funciona**: a etapa final (linhas → Cypher → grafo) roda sobre uma
  **leitura federada** do lake do cliente. O grafo só recebe dicts — não sabe de onde vêm.
- **DuckDB é a engine mais flexível**: lê **os dois** formatos in-process, sem infra extra;
  é o mesmo motor que o `memory_worker` já usa (só troca o `_build_select` do reader).
- **Trino** serve quando o cliente já tem Trino / lake distribuído e lê Delta (+ Iceberg,
  Snowflake via connectors), mas **não lê DuckLake** e exige catálogo/metastore.
- 🛑 **A engine é ditada pelo formato do cliente:** DuckLake ⇒ **DuckDB obrigatório**;
  Delta/Iceberg/Snowflake ⇒ DuckDB **ou** Trino.

## Caveats (a cravar)

- Testado em **disco local** simulando o lake. Contra **S3/MinIO real** (Databricks/
  Snowflake do cliente) é troca de config — falta rodar (o folder 05 já provou `s3://` no DuckDB).
- **Dedup / freshness / custo** de reler o lake do cliente a cada carga (federação relê a
  fonte toda vez) — em aberto.
- **Snowflake** via DuckDB depende de extensão experimental; via Trino é connector nativo.
- **ACL / Unity Catalog managed** — exige credencial de leitura do storage do cliente.

> Decisão DuckDB vs Trino: [pontos-a-verificar §2](../../docs/arquitetura/2.0-lake-aberto/pontos-a-verificar.md).
