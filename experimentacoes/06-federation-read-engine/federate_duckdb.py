"""13 · Federation — ler a CLEAN do CLIENTE (Delta e DuckLake) e alimentar o grafo,
SEM ETL (sem copiar pra nossa RAW).

Cenário: a tabela CLEAN é do cliente, ele optou por não fazer ETL. A gente lê
DIRETO o lake dele com uma engine (aqui: DuckDB) e faz o mapping → FalkorDB.
Nada é gravado na nossa RAW/CLEAN — o dado vai lake → RAM → grafo.

    LAKE DO CLIENTE (Delta | DuckLake)  --DuckDB lê direto-->  linhas  --Cypher-->  FalkorDB

Rodar:  ../.venv/bin/python federate_duckdb.py
Requer: FalkorDB em localhost:6380 (docker run -p 6380:6379 falkordb/falkordb).
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

import duckdb
import falkordb
from deltalake import write_deltalake
import pyarrow as pa

HERE = Path(__file__).parent.resolve()
LAKE = HERE / ".out" / "client_lake"          # simula o object storage do cliente
DELTA_DIR = LAKE / "delta" / "orders"
DUCKLAKE_CAT = LAKE / "ducklake" / "catalog.ducklake"
DUCKLAKE_DATA = LAKE / "ducklake" / "data"
FALKOR_PORT = 6380
N = 50


def client_rows() -> pa.Table:
    ids = list(range(1, N + 1))
    return pa.table({
        "id": ids,
        "customer_email": [f"user{i}@example.com" for i in ids],
        "amount": [i * 1.5 for i in ids],
        "updated_at": ["2026-07-01T00:00:00"] * N,
    })


def build_client_lake() -> None:
    """Cria a CLEAN do cliente nos dois formatos. (Isto é o lake DELE, não nosso.)"""
    shutil.rmtree(LAKE, ignore_errors=True)
    DELTA_DIR.parent.mkdir(parents=True, exist_ok=True)
    DUCKLAKE_DATA.mkdir(parents=True, exist_ok=True)
    rows = client_rows()

    # (a) CLEAN em Delta
    write_deltalake(str(DELTA_DIR), rows, mode="overwrite")

    # (b) CLEAN em DuckLake (catálogo SQL + parquet)
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake")
    con.execute(f"ATTACH 'ducklake:{DUCKLAKE_CAT}' AS lake (DATA_PATH '{DUCKLAKE_DATA}/')")
    con.register("rows", rows)
    con.execute("CREATE TABLE lake.orders AS SELECT * FROM rows")
    con.execute("DETACH lake")
    con.close()


# ─────────── federation reads: DuckDB lê DIRETO o lake do cliente ───────────

def read_delta_federated() -> list[dict]:
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta")
    rows = con.execute(
        f"SELECT id, customer_email, amount, updated_at FROM delta_scan('{DELTA_DIR}')"
    ).to_arrow_table().to_pylist()
    con.close()
    return rows


def read_ducklake_federated() -> list[dict]:
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake")
    # READ_ONLY: a gente é só consumidor do lake do cliente
    con.execute(f"ATTACH 'ducklake:{DUCKLAKE_CAT}' AS client (READ_ONLY)")
    rows = con.execute(
        "SELECT id, customer_email, amount, updated_at FROM client.orders"
    ).to_arrow_table().to_pylist()
    con.close()
    return rows


# ─────────── mapping → grafo (idêntico ao memory_worker: 1 MERGE por linha) ───────────

def load_graph(rows: list[dict], graph_name: str) -> int:
    g = falkordb.FalkorDB(host="localhost", port=FALKOR_PORT).select_graph(graph_name)
    g.query("MATCH (n) DETACH DELETE n")
    for r in rows:
        g.query(
            "MERGE (o:Order {id:$id}) "
            "SET o.email=$email, o.amount=$amount, o.updated_at=$updated_at",
            {"id": r["id"], "email": r["customer_email"],
             "amount": r["amount"], "updated_at": str(r["updated_at"])},
        )
    return g.query("MATCH (o:Order) RETURN count(o)").result_set[0][0]


def main() -> None:
    print(f"duckdb {duckdb.__version__}")
    build_client_lake()
    print(f"CLEAN do cliente criada (Delta + DuckLake), {N} linhas\n")

    for label, reader, gname in [
        ("DELTA",    read_delta_federated,    "fed_delta"),
        ("DUCKLAKE", read_ducklake_federated, "fed_ducklake"),
    ]:
        rows = reader()
        assert len(rows) == N, f"{label}: leu {len(rows)}"
        nodes = load_graph(rows, gname)
        sample = rows[0]
        print(f"[{label}] DuckDB leu {len(rows)} linhas direto do lake (sem copiar) "
              f"→ {nodes} nós :Order no FalkorDB")
        print(f"          sample: id={sample['id']} email={sample['customer_email']} amount={sample['amount']}")
        assert nodes == N, f"{label}: grafo com {nodes} nós"

    print("\n✅ Federation OK (DuckDB): Delta E DuckLake lidos direto do lake do cliente → grafo, sem ETL.")


if __name__ == "__main__":
    main()
