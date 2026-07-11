"""13 · Federation via TRINO — ler a CLEAN Delta do cliente e alimentar o grafo.

Complementa federate_duckdb.py com a outra engine do diagrama (Trino). Prova que
uma engine de federação "de verdade" (distribuída, poliglota) lê o Delta do lake
do cliente direto e o mesmo mapping alimenta o FalkorDB — sem ETL.

    Trino (connector delta_lake) --lê o Delta do cliente--> linhas --Cypher--> FalkorDB

⚠️ DuckLake NÃO é testável aqui: o Trino não tem connector DuckLake (formato é
específico do DuckDB). Ver RESULTADOS.md.

Pré-req: container exp-trino em :8085 e exp-falkor em :6380 (ver README).
Rodar:  ../.venv/bin/python federate_trino.py
"""
from __future__ import annotations

import time

import falkordb
import trino

FALKOR_PORT = 6380
N = 50


def trino_conn():
    return trino.dbapi.connect(host="localhost", port=8085, user="fed", catalog="delta")


def wait_trino(timeout=90) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            cur = trino_conn().cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            return
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(3)
    raise RuntimeError(f"Trino não respondeu em {timeout}s: {last}")


def register_client_delta() -> None:
    """Registra a tabela Delta do cliente no catálogo do Trino (não copia dado)."""
    cur = trino_conn().cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS delta.fed")
    cur.fetchall()
    # idempotente: desregistra se já existir
    try:
        cur.execute("CALL delta.system.unregister_table(schema_name => 'fed', table_name => 'orders')")
        cur.fetchall()
    except Exception:
        pass
    cur.execute(
        "CALL delta.system.register_table("
        "schema_name => 'fed', table_name => 'orders', "
        "table_location => 'local:///lake/delta/orders')"
    )
    cur.fetchall()


def read_delta_via_trino() -> list[dict]:
    cur = trino_conn().cursor()
    cur.execute("SELECT id, customer_email, amount, updated_at FROM delta.fed.orders")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


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
    print("aguardando Trino...")
    wait_trino()
    print("Trino no ar. Registrando a tabela Delta do cliente...")
    register_client_delta()
    rows = read_delta_via_trino()
    print(f"[DELTA/Trino] Trino leu {len(rows)} linhas direto do lake do cliente (sem copiar)")
    assert len(rows) == N, len(rows)
    nodes = load_graph(rows, "fed_delta_trino")
    print(f"[DELTA/Trino] → {nodes} nós :Order no FalkorDB")
    assert nodes == N, nodes
    print("\n✅ Federation OK (Trino): Delta do cliente lido direto → grafo, sem ETL.")
    print("   (DuckLake não aplicável — Trino não tem connector DuckLake.)")


if __name__ == "__main__":
    main()
