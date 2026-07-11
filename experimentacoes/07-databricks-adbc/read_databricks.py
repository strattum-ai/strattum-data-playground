"""07 · Federation read test — Databricks → Arrow → DuckDB.

Testa a hipótese da 2.0: ler uma tabela do Databricks e consumir no DuckDB
(zero-copy via Arrow) — o caminho "DuckDB + ADBC" da federation, sem ETL.

Config por ENV (NUNCA hardcode o token no arquivo):
  export DATABRICKS_SERVER_HOSTNAME="dbc-xxxx.cloud.databricks.com"
  export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxxxxx"
  export DATABRICKS_TOKEN="dapi..."                       # PAT
  export DATABRICKS_TABLE="main.default.strattum_sample_orders"

Rodar:
  pip install "databricks-sql-connector" duckdb pyarrow
  python read_databricks.py
"""
from __future__ import annotations
import os
import duckdb
from databricks import sql

HOST  = os.environ["DATABRICKS_SERVER_HOSTNAME"]
PATH  = os.environ["DATABRICKS_HTTP_PATH"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
TABLE = os.environ.get("DATABRICKS_TABLE", "main.default.strattum_sample_orders")

print(f"Conectando ao Databricks {HOST} …")
with sql.connect(server_hostname=HOST, http_path=PATH, access_token=TOKEN) as conn:
    with conn.cursor() as cur:
        # 1. leitura completa da tabela -> Arrow (zero-copy)
        cur.execute(f"SELECT * FROM {TABLE}")
        arrow = cur.fetchall_arrow()
        print(f"[Databricks] {TABLE}: {arrow.num_rows} linhas · {arrow.num_columns} colunas")
        print(f"[Databricks] colunas: {arrow.schema.names}")

        # 2. leitura INCREMENTAL — o filtro é empurrado (pushdown) pro Databricks
        cur.execute(f"SELECT * FROM {TABLE} WHERE updated_at > TIMESTAMP '2026-04-01 00:00:00'")
        arrow_delta = cur.fetchall_arrow()
        print(f"[Databricks] delta (updated_at > 2026-04-01): {arrow_delta.num_rows} linhas")

# 3. DuckDB consome o Arrow (zero-copy) e roda SQL/transform in-process
con = duckdb.connect()
con.register("dbx", arrow)                 # a tabela do Databricks vira uma view no DuckDB
print("\n[DuckDB] agregando o que veio do Databricks:")
for row in con.execute("""
    SELECT status, count(*) AS n, round(sum(amount), 2) AS total
    FROM dbx GROUP BY status ORDER BY n DESC
""").fetchall():
    print("  ", row)

print("\n[DuckDB] amostra (3 linhas):")
for row in con.execute("SELECT id, external_id, customer_name, amount, updated_at FROM dbx LIMIT 3").fetchall():
    print("  ", row)

print("\n✅ OK — Databricks → Arrow → DuckDB funcionou (é o caminho da federation).")

# ── Alternativa PURA ADBC (opcional, a validar maturidade) ────────────────────
# O caminho acima usa o databricks-sql-connector (oficial, Arrow-native). Pra testar
# o driver ADBC puro da comunidade:
#   pip install adbc-driver-manager
#   import adbc_driver_manager.dbapi as adbc
#   conn = adbc.connect(driver="<databricks adbc driver .so>", db_kwargs={...})
# — é o ponto "maturidade do ADBC" da tarefa 03; o connector oficial já prova o conceito.
