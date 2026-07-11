"""§3 · Tudo em Delta Lake — pipeline completo até o grafo (abordagem "ponte").

Contraparte da variante DuckLake, mas com **Delta Lake** em RAW e CLEAN, e vai até o
FalkorDB. Fonte SQLite (= "Postgres") pra rodar self-contained. Usa a **ponte** (dbt
materializa scratch → Python `write_deltalake`); a alternativa sem ponte (plugin) está
em `dbt_delta_test.py` (§4).

    fonte --dlt--> RAW (Delta) --dbt+py--> CLEAN (Delta) --Cypher--> FalkorDB
      overwrite (full)  e depois  incremental (só o delta)

Rodar:  ../.venv/bin/python delta_pipeline.py
Requer: FalkorDB em localhost:6380 (docker run -p 6380:6379 falkordb/falkordb).
"""
from __future__ import annotations
import os, sys, shutil, subprocess, time
sys.path.insert(0, "/Users/allanfraga/Repos/strattum/experimentacoes")
import exp, duckdb, dlt
from deltalake import DeltaTable, write_deltalake
from sqlalchemy import create_engine, text
from prefect import flow, task

# ─────────────── paths / config ───────────────
ART     = f"{exp.DATA}/delta-tudo"
LAKE    = f"{ART}/lake"
RAW     = f"{LAKE}/raw/orders"          # tabela Delta da RAW
CLEAN   = f"{LAKE}/clean/orders"        # tabela Delta da CLEAN
SCRATCH = f"{ART}/scratch.duckdb"       # engine efêmero do dbt (descartável)
SRC     = f"sqlite:///{ART}/source.db"
DBT_DIR = f"{exp.EXP}/05-formato-storage-lake/dbt-delta-pipeline"
FALKOR_PORT = 6380
DBT = os.path.join(os.path.dirname(sys.executable), "dbt")

os.environ["PREFECT_LOGGING_LEVEL"] = "WARNING"
os.environ["RAW_ORDERS"] = RAW
os.environ["CLEAN_ORDERS"] = CLEAN

# perfil dbt: DuckDB como engine (lê Delta via delta_scan), banco scratch
open(f"{DBT_DIR}/profiles.yml", "w").write(
"""delta_clean:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "%s"
      schema: main
      extensions: [delta, httpfs]
""" % SCRATCH)


def reset_all():
    shutil.rmtree(ART, ignore_errors=True)
    os.makedirs(LAKE, exist_ok=True)


def seed(rows, start=0, day="2026-01-01"):
    eng = create_engine(SRC)
    with eng.begin() as cx:
        cx.execute(text("CREATE TABLE IF NOT EXISTS orders "
                        "(id INTEGER PRIMARY KEY, updated_at VARCHAR, amount DOUBLE)"))
        for i in range(start, start + rows):
            cx.execute(text("INSERT INTO orders (id, updated_at, amount) VALUES (:i,:u,:a)"),
                       {"i": i, "u": f"{day}T00:00:{i % 60:02d}", "a": i * 1.5})
    eng.dispose()


def dlt_source(incremental=False):
    from dlt.sources.sql_database import sql_database
    s = sql_database(credentials=SRC, table_names=["orders"], backend="sqlalchemy")
    s.orders.apply_hints(table_format="delta")          # RAW em Delta
    if incremental:
        s.orders.apply_hints(incremental=dlt.sources.incremental("updated_at"))
    return s


def dlt_pipe():
    # filesystem grava em {bucket_url}/{dataset_name}/{table} → {LAKE}/raw/orders
    return dlt.pipeline("delta_tudo",
                        destination=dlt.destinations.filesystem(bucket_url=LAKE),
                        dataset_name="raw", pipelines_dir=f"{ART}/dlt")


def dbt_run(incremental: bool):
    """dbt computa a CLEAN (lendo a RAW Delta). Retorna Arrow com o resultado."""
    os.environ["CLEAN_INCREMENTAL"] = "1" if incremental else "0"
    r = subprocess.run([DBT, "run", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stdout + r.stderr
    con = duckdb.connect(SCRATCH, read_only=True)
    tbl = con.execute("SELECT * FROM main.clean_orders").arrow()
    con.close()
    return tbl


def count_delta(path):
    try:
        return DeltaTable(path).to_pyarrow_dataset().count_rows()
    except Exception:
        return "(vazio)"


# ─────────────── os 4 passos + grafo, como tasks Prefect ───────────────
@task
def p1_raw_overwrite():
    dlt_pipe().run(dlt_source(), write_disposition="replace")
    return count_delta(RAW)

@task
def p2_clean_overwrite():
    tbl = dbt_run(incremental=False)
    write_deltalake(CLEAN, tbl, mode="overwrite", schema_mode="overwrite")
    return count_delta(CLEAN)

@task
def p3_raw_incremental():
    seed(20, start=100, day="2026-02-01")
    dlt_pipe().run(dlt_source(incremental=True), write_disposition="merge", primary_key="id")
    return count_delta(RAW)

@task
def p4_clean_incremental():
    tbl = dbt_run(incremental=True)                    # só o delta
    DeltaTable(CLEAN).merge(                           # upsert por id (Python, não dbt)
        source=tbl, predicate="t.id = s.id", source_alias="s", target_alias="t",
    ).when_matched_update_all().when_not_matched_insert_all().execute()
    return count_delta(CLEAN)

@task
def p5_write_falkor():
    import falkordb
    g = falkordb.FalkorDB(host="localhost", port=FALKOR_PORT).select_graph("delta_demo")
    g.query("MATCH (n) DETACH DELETE n")
    rows = DeltaTable(CLEAN).to_pyarrow_table().to_pylist()
    for row in rows:                                   # 1 MERGE por linha (como o memory_worker)
        g.query("MERGE (o:Order {id:$id}) SET o.amount=$amount, o.updated_at=$updated_at",
                {"id": row["id"], "amount": row["amount"], "updated_at": str(row["updated_at"])})
    return g.query("MATCH (o:Order) RETURN count(o)").result_set[0][0]


@flow(name="delta-tudo-pipeline")
def delta_pipeline():
    reset_all(); seed(100)
    t = {}
    def timed(label, fn):
        t0 = time.perf_counter(); r = fn(); t[label] = time.perf_counter() - t0; return r
    raw1   = timed("1 raw overwrite",   p1_raw_overwrite)
    clean1 = timed("2 clean overwrite", p2_clean_overwrite)
    raw2   = timed("3 raw incremental", p3_raw_incremental)
    clean2 = timed("4 clean increment", p4_clean_incremental)
    nodes  = timed("5 write falkor",    p5_write_falkor)
    print("\n─── RESULTADO ───")
    print(f"RAW após overwrite     : {raw1}   (esperado 100)")
    print(f"CLEAN após overwrite   : {clean1}   (esperado 100)")
    print(f"RAW após incremental   : {raw2}   (esperado 120)")
    print(f"CLEAN após incremental : {clean2}   (esperado 120)")
    print(f"nós :Order no FalkorDB : {nodes}   (esperado 120)")
    print("\n─── TEMPOS (s) ───")
    for k, v in t.items():
        print(f"{k:20s}: {v:6.2f}")
    return {"raw": raw1, "clean": clean1, "raw_inc": raw2, "clean_inc": clean2, "falkor": nodes}


if __name__ == "__main__":
    print("duckdb", duckdb.__version__, "| dlt", dlt.__version__, "| deltalake", __import__("deltalake").__version__)
    delta_pipeline()
