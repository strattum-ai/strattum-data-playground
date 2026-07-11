# 07 · Ler do Databricks com DuckDB (via Arrow) — passo a passo

> Testa o caminho de **federation** da 2.0: ler uma tabela do **Databricks** e consumir no
> **DuckDB** (zero-copy via **Arrow**), sem ETL. Prova que `Databricks → Arrow → DuckDB`
> funciona — a base pra depois `→ grafo`. Script: `read_databricks.py`.

## O que você precisa juntar no Databricks (3 coisas + a tabela)

### 1. Server hostname + HTTP path (do SQL Warehouse)
- No Databricks: **SQL** (menu lateral) → **SQL Warehouses** → clique num warehouse
  (crie/ligue um *Serverless* pequeno se não tiver) → aba **Connection details**.
- Copie **Server hostname** (ex.: `dbc-abc123.cloud.databricks.com`) e
  **HTTP path** (ex.: `/sql/1.0/warehouses/abcdef123456`).
- ⚠️ O warehouse precisa estar **Running** (liga na hora de rodar).

### 2. Access token (PAT)
- Canto superior direito (seu avatar) → **Settings** → **Developer** → **Access tokens** →
  **Generate new token** → copie (começa com `dapi…`).
- ⚠️ **Não** cole o token em arquivo nem me mande — vai só numa variável de ambiente.

### 3. A tabela (onde estão seus dados)
- **Catalog** (menu lateral, Catalog Explorer) → navegue até a tabela. O nome completo é
  `catalog.schema.tabela` (ex.: `main.default.strattum_sample_orders` — a que você criou do CSV).
- Anote o nome completo.

## Rodar

```bash
cd experimentacoes
python3.12 -m venv .venv && source .venv/bin/activate     # se ainda não tiver
pip install "databricks-sql-connector" duckdb pyarrow

# preencha com os seus valores (o token só aqui, na sessão do shell):
export DATABRICKS_SERVER_HOSTNAME="dbc-xxxx.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxxxxx"
export DATABRICKS_TOKEN="dapi..."
export DATABRICKS_TABLE="main.default.strattum_sample_orders"

python 07-databricks-adbc/read_databricks.py
```

## O que o script faz (e o que estamos validando)

1. Conecta no Databricks e faz `SELECT * FROM <tabela>` → **Arrow** (`fetchall_arrow`).
2. Faz uma **leitura incremental** (`WHERE updated_at > …`) — o filtro é **empurrado pro
   Databricks** (pushdown), provando que dá pra ler só o delta.
3. **DuckDB consome o Arrow** (zero-copy) e roda um `GROUP BY` — provando que o dado do
   Databricks vira uma "tabela" no DuckDB, pronta pra virar dict → grafo.

## O que me mandar
Cola aqui a **saída do script** (as linhas `[Databricks] …`, `[DuckDB] …` e o `✅ OK`). Com
isso eu preencho o `RESULTADOS.md` com o resumo e o que ficou validado / o que falta (ADBC
puro, ida ao grafo).

> Nota: usamos o `databricks-sql-connector` (oficial, Arrow-native) — é o caminho confiável e
> já prova o conceito "DuckDB + Arrow". O **driver ADBC puro** (comunidade) é o item de
> *maturidade a validar* da [tarefa 03](../../documents/arquitetura/2.0-lake-aberto/tarefas/03-federation/).
