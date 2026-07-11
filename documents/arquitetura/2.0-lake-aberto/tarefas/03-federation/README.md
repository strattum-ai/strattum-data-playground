# 03 · Federation — ler o lake do cliente sem ETL

> Para a empresa que **já tem lake** (Databricks/Snowflake), não faz sentido copiar tudo pra
> nossa RAW/CLEAN. Uma engine lê o lake dele **direto → grafo**, sem ETL. O grafo não sabe a
> origem — só recebe dicts (mesmo loop do `memory-worker`).
>
> Base: [descobertas §5](../../descobertas.md) · experimento:
> [folder 06](../../../../../experimentacoes/06-federation-read-engine/RESULTADOS.md).

Legenda: 🛑 a fazer · ⚖️ decisão · 🔗 detalhe noutro doc

---

## ✅ Decisão: DuckDB + ADBC — **sem Trino**

O engine é o **DuckDB** — o mesmo que o `memory-worker` já roda in-process — e a leitura de
**Snowflake/Databricks** entra via **ADBC** (Arrow Database Connectivity): a extensão
[`adbc`](https://github.com/columnar-tech/duckdb-adbc-client) + o driver instalado com
`dbc install <driver>` leem o warehouse/lake e entregam **Arrow**, que o DuckDB consome
**zero-copy**. Isso **fecha o buraco antigo** ("DuckDB não lê Snowflake nativo").

> **✅ Validado contra o Databricks real** ([folder 07](../../../../../experimentacoes/07-databricks-adbc/RESULTADOS.md)):
> `read_adbc('profile://dbx', 'SELECT … FROM workspace.default.strattum_sample_orders')` leu 200
> linhas, com o `GROUP BY` rodando pushdown no Databricks.

| Fonte do cliente | Como o DuckDB lê |
|---|---|
| Delta (Databricks) | `delta_scan('s3://…')` / `uc_catalog` (Unity) |
| DuckLake | `ATTACH 'ducklake:…'` |
| Iceberg | `iceberg_scan` |
| **Snowflake / Databricks (SQL)** | **ADBC** (`read_adbc` / `ATTACH … TYPE adbc`) — ✅ validado |

- ✅ **DuckDB + ADBC** = zero infra nova (in-process), cobre lake **e** warehouse fechado.
- 🛑 **Trino está fora.** A extensão ADBC cobre o que justificava o Trino — não vamos rodar
  cluster/metastore. (Cliente em DuckLake, aliás, o Trino nem leria.)

## Dois modos de consumo da federation (os dois precisam funcionar)

A fonte federada (lida via **DuckDB + ADBC**) não serve só pra ir direto ao grafo — ela é uma
**fonte como qualquer outra**, e tem que funcionar nos dois caminhos:

1. **Direto → grafo** (no-ETL): o `memory-worker` lê a fonte federada e materializa nós/arestas.
   **Requisito:** é o **mesmo leitor** que lê a nossa CLEAN — a classe [`LakeStore`](../01-lakehouse/)
   trata `read(("fed", tabela))` igual a `read(("clean", tabela))` (mesma saída, dicts). A
   federation é **plugável no leitor existente**, não um caminho paralelo.
2. **Via dbt (federation layer)**: um model dbt **lê a fonte federada** (registrada como
   view/source) e a **combina** (JOIN) com **RAW** ou **ENRICHMENT** nossas, produzindo CLEAN.
   Ex.: enriquecer o `fct_deals` da federation com o `enrichment.company_profile` que a gente
   gerou por LLM. Aqui a federation vira uma **camada a mais que o dbt cruza**, não só um atalho.

> Ou seja: a mesma capacidade **DuckDB+ADBC** cobre **empresa sem lake** (lê a nossa
> CLEAN/DuckLake → grafo) **e** **empresa com lake** (lê a fonte do cliente → grafo **ou** dbt).
> Um leitor, várias origens.

## Como isso roda em produção (config na UI → dbt + grafo)

No teste (folder 07) o profile foi criado à mão. Em produção o **cliente configura na UI** e a
plataforma faz o resto. Reusa o que **já existe** (o mesmo caminho dos conectores):

**1. UI → salvar.** O cliente preenche a conexão da federation no console (Databricks/Snowflake:
host, http_path/warehouse, catalog; token). O **Catalog API** grava:
- os campos **não-secretos** no **connector registry** (`connector_registry.json` — o mesmo
  [`registry.py`](../../../../../strattum-data/services/pipelines/src/connectors/utils/registry.py) que os conectores leem);
- o **token** no **secrets store** (`strattum_core.secrets.store` — backend JSON local ou
  **AWS Secrets Manager**, via `SECRET_BACKEND`). **Nunca** em texto no registry/git.

**2. Por run → materializar a conexão.** Antes do dbt/worker rodar, um passo (o
`LakeStore.attach_federation`) lê config do registry + token do secrets store e **monta a
conexão ADBC**:
- monta a `uri` `databricks://token:<PAT>@host:443<http_path>` e ou (a) **renderiza um profile
  TOML efêmero** no dir de Profiles do ADBC, ou (b) passa a `uri` **inline** via
  `adbc_driver_manager` (sem arquivo). Profile efêmero por run, apagado no fim — o token nunca
  fica em disco versionado.

**3. dbt usa.** `on-run-start` instala/carrega a extensão (`INSTALL adbc FROM community; LOAD adbc;`)
e a imagem já tem o driver (`dbc install databricks`). O model faz:
```sql
{{ config(materialized='table') }}
SELECT f.*, e.company_profile
FROM read_adbc('profile://{{ var("fed_profile") }}',
     'SELECT * FROM workspace.default.fct_deals WHERE updated_at > {{ var("watermark") }}') f
LEFT JOIN {{ ref('company_enrichment') }} e USING (company_id)   -- combina com enrichment/raw
```

**4. Grafo usa.** `graph_mapping.yaml` com `source: fed/<tabela>` → `LakeStore.read(("fed", t))`
(a mesma `uri`/profile) → memory-worker → FalkorDB. Mesma config, dois consumidores.

> 💡 Um `LakeStore` por cliente/fonte: a UI define **qual driver** (`dbc install`) e **a uri**;
> a classe abstrai profile-vs-inline. Trocar de PAT pra OAuth/service principal depois = mudar
> a montagem da `uri`, não os models.

## Tarefas

- ✅ **Ler o Databricks via ADBC** — feito ([folder 07](../../../../../experimentacoes/07-databricks-adbc/RESULTADOS.md)): `read_adbc` leu 200 linhas com pushdown.
- 🛑 **Ida ao grafo:** ligar `fed/<tabela>` → `LakeStore.read(("fed", t))` → memory-worker → FalkorDB (o worker recebe os dicts do `read_adbc`).
- 🛑 **Leitor unificado:** `LakeStore.read(("fed", …))` = `read(("clean", …))` — worker não distingue origem. 🔗 [tarefa 01 §E](../01-lakehouse/).
- 🛑 **Federation como source do dbt:** model que faz `read_adbc(...)` + **JOIN com raw/enrichment** → clean (exemplo na seção "Como isso roda em produção").
- 🛑 **Wiring de produção:** UI (Catalog API) grava config no registry + **token no secrets store**; `LakeStore.attach_federation` **renderiza o profile/uri por run** (efêmero). `dbc install <driver>` na imagem + `INSTALL adbc` no `on-run-start` do dbt.
- 🛑 **Validar Snowflake via ADBC** (`dbc install snowflake`) — mesmo caminho do Databricks.
- 🛑 **Dedup / freshness / custo** da releitura + **ACL / Unity Catalog *managed*** (credencial de leitura do storage do cliente; suporte *managed* no DuckDB é novo).
- 🔗 Como o `memory-worker`/`catalog-api` apontam pro lake (via `LakeStore`): [migracao](../01-lakehouse/migracao.md) · [tarefa 01 §E](../01-lakehouse/).

---

## Referências / inspiração

- 📄 **Artigo (leitura recomendada):** [Announcing the DuckDB ADBC Extension](https://columnar.tech/blog/announcing-duckdb-adbc-extension/) — columnar.tech, jul/2026. É o que **destrava** a nossa federation.
- 💻 Extensão: [`columnar-tech/duckdb-adbc-client`](https://github.com/columnar-tech/duckdb-adbc-client) · CLI de drivers: [`dbc`](https://columnar.tech/dbc) · docs dos drivers: [docs.adbc-drivers.org](https://docs.adbc-drivers.org/).
- 🧪 Nosso teste que validou: [experimentacoes/07-databricks-adbc](../../../../../experimentacoes/07-databricks-adbc/RESULTADOS.md).

**Por que muda o jogo pra nós (o resumo):**
- **Antes:** DuckDB só lia bancos externos por **extensão vendor-específica** (postgres/mysql/snowflake…) e **não existia pra Databricks/Redshift/Oracle** → federation exigiria **Trino (cluster)**, contra a nossa filosofia single-node.
- **Agora:** a extensão **vira o DuckDB num cliente ADBC** → lê **qualquer** sistema com driver ADBC (30+: Databricks, Snowflake, BigQuery, Redshift…) por **uma interface só**, com `dbc install <driver>`. **Zero infra nova** (o mesmo DuckDB do worker/dbt), **Arrow zero-copy**, sem Trino.
- **Efeito:** federation deixa de ser "precisa de cluster" e vira "uma linha na engine que já rodamos" — e uma engine única cobre ingestão, transform, federation e grafo. Reforça o produto **DuckLake + DuckDB numa caixa**.

**Termos:**
- **ADBC** (Arrow Database Connectivity): API universal de banco sobre Arrow (colunar) — como JDBC/ODBC, mas zero-copy. Um padrão, N drivers.
- **`dbc`**: gerenciador de drivers ADBC da columnar.tech ("apt pra drivers ADBC") — `dbc install databricks` baixa a lib nativa do driver.
