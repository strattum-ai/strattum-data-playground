"""DuckLake na prática — ver os arquivos, o catálogo, dlt e dbt.

Pipeline (mesma arquitetura da Strattum, mas em DuckLake):
    fonte (SQLite) --dlt--> RAW (lake.raw.orders) --dbt--> CLEAN (lake.main_clean.orders_clean)
                     destination=ducklake            +database='lake' (materializa nativo)

DuckLake = Parquet no storage (pasta `lake/`) + metadados num catálogo SQL (`catalog.ducklake`).
Este script mostra os DOIS: os arquivos gerados e o conteúdo do catálogo.

Rodar:  ../.venv/bin/python ducklake_demo.py
"""
from __future__ import annotations
import os, sys, shutil, subprocess, glob
sys.path.insert(0, "/Users/allanfraga/Repos/strattum/experimentacoes")
import exp, duckdb, dlt
from dlt.sources.sql_database import sql_database
from dlt.destinations import ducklake
from sqlalchemy import create_engine, text

ART      = f"{exp.DATA}/ducklake-demo"
CATALOG  = f"{ART}/catalog.ducklake"          # catálogo (metadados). Em prod: Postgres.
LAKE_DIR = f"{ART}/lake"                       # storage (Parquet). Em prod: MinIO/S3.
SRC      = f"sqlite:///{ART}/source.db"        # "Postgres" simulado
DBT_DIR  = f"{exp.EXP}/05-formato-storage-lake/dbt-ducklake"
DBT      = os.path.join(os.path.dirname(sys.executable), "dbt")
os.environ["PREFECT_LOGGING_LEVEL"] = "WARNING"

# perfil dbt: DuckDB em memória (só engine) + ATTACH do catálogo DuckLake
os.environ["DUCKLAKE_CATALOG"] = CATALOG
open(f"{DBT_DIR}/profiles.yml", "w").write(
f"""ducklake_demo:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ":memory:"
      extensions: [ducklake]
      attach:
        - path: "ducklake:{{{{ env_var('DUCKLAKE_CATALOG') }}}}"
          alias: lake
""")


def reset():
    shutil.rmtree(ART, ignore_errors=True)
    os.makedirs(LAKE_DIR, exist_ok=True)


def seed(rows, start=0, day="2026-01-01"):
    eng = create_engine(SRC)
    with eng.begin() as cx:
        cx.execute(text("CREATE TABLE IF NOT EXISTS orders "
                        "(id INTEGER PRIMARY KEY, updated_at VARCHAR, amount DOUBLE, email VARCHAR)"))
        for i in range(start, start + rows):
            cx.execute(text("INSERT INTO orders (id, updated_at, amount, email) VALUES (:i,:u,:a,:e)"),
                       {"i": i, "u": f"{day}T00:00:{i % 60:02d}", "a": i * 1.5, "e": f"  User{i}@X.com "})
    eng.dispose()


def lake_dest():
    return ducklake(credentials=dict(
        ducklake_name="lake",
        catalog=f"duckdb:///{CATALOG}",
        storage={"bucket_url": LAKE_DIR},
    ))


def dlt_pipe():
    return dlt.pipeline("ducklake_demo", destination=lake_dest(),
                        dataset_name="raw", pipelines_dir=f"{ART}/dlt")


def dlt_source(incremental=False):
    s = sql_database(credentials=SRC, table_names=["orders"], backend="sqlalchemy")
    if incremental:
        s.orders.apply_hints(incremental=dlt.sources.incremental("updated_at"))
    return s


def dbt_run(full_refresh=False):
    cmd = [DBT, "run", "--project-dir", DBT_DIR, "--profiles-dir", DBT_DIR]
    if full_refresh:
        cmd.append("--full-refresh")
    r = subprocess.run(cmd, capture_output=True, text=True)
    assert r.returncode == 0, (r.stdout + r.stderr)[-1500:]
    print("   dbt:", r.stdout.strip().splitlines()[-1])


# ─────────────── inspeção: arquivos + catálogo ───────────────
def show_files(title):
    print(f"\n📁 {title} — arquivos em {LAKE_DIR.split('/')[-1]}/ (storage)")
    files = sorted(glob.glob(f"{LAKE_DIR}/**/*", recursive=True))
    pq = [f for f in files if f.endswith(".parquet")]
    for f in pq:
        rel = f.replace(ART + "/", "")
        print(f"   {os.path.getsize(f):>7} B  {rel}")
    print(f"   → {len(pq)} arquivo(s) parquet")
    print(f"   catálogo: catalog.ducklake = {os.path.getsize(CATALOG)} B (metadados em SQL)")


def show_catalog(title):
    print(f"\n🗂  {title} — dentro do catálogo (tabelas de metadados DuckLake)")
    con = duckdb.connect(CATALOG, read_only=True)
    tbls = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'ducklake_%' ORDER BY 1"
    ).fetchall()]
    print("   tabelas de metadados:", ", ".join(tbls))
    for t in ("ducklake_snapshot", "ducklake_table", "ducklake_data_file"):
        if t in tbls:
            n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            print(f"\n   • {t} ({n} linha(s)):")
            cols = [d[0] for d in con.execute(f"SELECT * FROM {t} LIMIT 0").description]
            keep = [c for c in cols if c in (
                "snapshot_id","schema_version","snapshot_time","table_id","table_name",
                "schema_name","data_file_id","path","record_count","file_size_bytes")]
            keep = keep or cols[:4]
            for row in con.execute(f"SELECT {', '.join(keep)} FROM {t} ORDER BY 1 LIMIT 8").fetchall():
                print("     ", dict(zip(keep, row)))
    con.close()


def count(table, schema):
    con = duckdb.connect(); con.execute("INSTALL ducklake; LOAD ducklake")
    con.execute(f"ATTACH 'ducklake:{CATALOG}' AS lake (READ_ONLY)")
    n = con.execute(f"SELECT count(*) FROM lake.{schema}.{table}").fetchone()[0]
    con.close(); return n


if __name__ == "__main__":
    print("duckdb", duckdb.__version__, "| dlt", dlt.__version__)
    reset(); seed(100)

    print("\n=== 1) dlt: fonte → RAW (destination=ducklake, replace) ===")
    dlt_pipe().run(dlt_source(), write_disposition="replace")
    print("   RAW.orders =", count("orders", "raw"))
    show_files("após dlt RAW")
    show_catalog("após dlt RAW")

    print("\n=== 2) dbt: RAW → CLEAN (--full-refresh, materializa nativo no lake) ===")
    dbt_run(full_refresh=True)
    print("   CLEAN.orders_clean =", count("orders_clean", "main_clean"))
    show_files("após dbt CLEAN")

    print("\n=== 3) incremental: +20 na fonte → dlt merge → dbt incremental ===")
    seed(20, start=100, day="2026-02-01")
    dlt_pipe().run(dlt_source(incremental=True), write_disposition="merge", primary_key="id")
    dbt_run(full_refresh=False)
    print("   RAW =", count("orders", "raw"), "| CLEAN =", count("orders_clean", "main_clean"), "(esperado 120)")
    show_files("após incremental")
    show_catalog("após incremental (repare nos snapshots = versões)")

    print("\n=== 4) consumir: SELECT no lake.clean + time travel ===")
    con = duckdb.connect(); con.execute("INSTALL ducklake; LOAD ducklake")
    con.execute(f"ATTACH 'ducklake:{CATALOG}' AS lake (READ_ONLY)")
    print("   amostra clean:", con.execute(
        "SELECT id, amount, email FROM lake.main_clean.orders_clean ORDER BY id LIMIT 3").fetchall())
    snaps = con.execute("SELECT count(*) FROM lake.main_clean.orders_clean").fetchone()[0]
    print("   linhas clean agora:", snaps)
    con.close()
    print("\n✅ pronto — dados em Parquet (lake/), metadados no catálogo (catalog.ducklake)")
