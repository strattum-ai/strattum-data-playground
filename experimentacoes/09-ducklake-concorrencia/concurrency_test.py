"""09 · DuckLake — escrita concorrente: catálogo .duckdb (arquivo) vs Postgres.

Dois conectores em PROCESSOS separados escrevem no MESMO DuckLake ao mesmo tempo:
  - conectorA lê MySQL  (srcdb.customers) -> lake.raw_customers_mysql
  - conectorB lê Postgres (src.orders)    -> lake.raw_orders_pg
Dados vão pro MinIO (s3://). Compara os dois backends de catálogo.

Achado: catálogo-arquivo (.duckdb) é single-writer (o 2º conector falha no lock);
catálogo Postgres deixa os dois commitarem concorrentemente.

Pré-req (containers): exp-minio (:9000), exp-mysql (:3307), strattum-postgres (:5432),
DB `ducklake_catalog` no Postgres. Ver README.md.
Rodar:  ../.venv/bin/python concurrency_test.py
"""
from __future__ import annotations

import multiprocessing as mp
import shutil
import time
from pathlib import Path

PWPG = next(l.split("=", 1)[1].strip() for l in
            open("/Users/allanfraga/Repos/strattum/strattum-deploy/starter/.env")
            if l.startswith("POSTGRES_PASSWORD="))
HERE = Path(__file__).parent.resolve()
FILE_CAT = HERE / ".out" / "file_catalog"
PG_CAT = f"ducklake:postgres:dbname=ducklake_catalog host=localhost port=5432 user=strattum password={PWPG}"


def _con():
    import duckdb
    c = duckdb.connect()
    c.execute("INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs; "
              "INSTALL postgres; LOAD postgres; INSTALL mysql; LOAD mysql;")
    c.execute("CREATE OR REPLACE SECRET minio (TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin', "
              "ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false)")
    return c


def connector(name, source, catalog_uri, data_path, start_ts, q):
    """Um 'conector': lê a fonte e materializa uma tabela raw no DuckLake."""
    step = "start"
    try:
        c = _con()
        step = "attach_lake(RW)"                       # <- catálogo-arquivo: o 2º falha aqui (lock)
        c.execute(f"ATTACH '{catalog_uri}' AS lake (DATA_PATH '{data_path}')")
        step = "read_source"
        if source == "mysql":
            c.execute("ATTACH 'host=localhost port=3307 user=root password=root database=srcdb' AS s (TYPE mysql, READ_ONLY)")
            c.execute("CREATE TEMP TABLE t AS SELECT * FROM s.customers")
            tbl = "raw_customers_mysql"
        else:
            c.execute(f"ATTACH 'host=localhost port=5432 dbname=demo_source user=strattum password={PWPG}' AS s (TYPE postgres, READ_ONLY)")
            c.execute("CREATE TEMP TABLE t AS SELECT * FROM s.src.orders")
            tbl = "raw_orders_pg"
        while time.time() < start_ts:                  # sincroniza: os dois escrevem juntos
            time.sleep(0.01)
        step = f"write lake.{tbl}"
        c.execute(f"CREATE OR REPLACE TABLE lake.{tbl} AS SELECT * FROM t")
        n = c.execute(f"SELECT count(*) FROM lake.{tbl}").fetchone()[0]
        c.close()
        q.put((name, "OK", f"lake.{tbl} = {n} linhas"))
    except Exception as e:
        q.put((name, "FALHOU", f"[{step}] {type(e).__name__}: {str(e)[:150]}"))


def bootstrap(catalog_uri, data_path):
    """Cria o lake UMA vez (inicializa os metadados) antes dos writers concorrentes.
    (Inicializar um catálogo vazio com 2 writers ao mesmo tempo é corrida — faça 1x.)"""
    c = _con()
    c.execute(f"ATTACH '{catalog_uri}' AS lake (DATA_PATH '{data_path}')")
    c.execute("CREATE OR REPLACE TABLE lake._bootstrap AS SELECT 1 AS ok")
    c.close()


def reset_pg_catalog():
    import duckdb
    c = duckdb.connect(); c.execute("INSTALL postgres; LOAD postgres;")
    c.execute(f"ATTACH 'host=localhost port=5432 dbname=ducklake_catalog user=strattum password={PWPG}' AS m (TYPE postgres)")
    for (t,) in c.execute("SELECT table_name FROM information_schema.tables WHERE table_catalog='m' AND table_name LIKE 'ducklake_%'").fetchall():
        c.execute(f"DROP TABLE IF EXISTS m.{t} CASCADE")
    c.close()


def run_scenario(label, catalog_uri, data_path):
    print(f"\n{'='*72}\n{label}\n  dados: {data_path}\n{'='*72}")
    bootstrap(catalog_uri, data_path)
    print("  (catálogo inicializado 1x; agora 2 conectores escrevem CONCORRENTEMENTE)")
    q = mp.Queue()
    start = time.time() + 3.0
    procs = [
        mp.Process(target=connector, args=("conectorA(MySQL)", "mysql", catalog_uri, data_path, start, q)),
        mp.Process(target=connector, args=("conectorB(Postgres)", "postgres", catalog_uri, data_path, start, q)),
    ]
    for p in procs: p.start()
    for p in procs: p.join()
    res = sorted(q.get() for _ in procs)
    for name, status, msg in res:
        print(f"  {'✅' if status == 'OK' else '❌'} {name}: {status} — {msg}")
    ok = sum(1 for _, s, _ in res if s == "OK")
    print(f"  → {ok}/2 conectores escreveram concorrentemente")
    return ok


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)
    shutil.rmtree(FILE_CAT, ignore_errors=True); FILE_CAT.mkdir(parents=True)
    reset_pg_catalog()

    a = run_scenario("CENÁRIO A — catálogo .duckdb (ARQUIVO) + dados no MinIO",
                     f"ducklake:{FILE_CAT}/catalog.ducklake", "s3://lake/scenarioA/")
    b = run_scenario("CENÁRIO B — catálogo POSTGRES + dados no MinIO",
                     PG_CAT, "s3://lake/scenarioB/")
    print(f"\n{'#'*72}\nRESUMO:  catálogo .duckdb = {a}/2   |   catálogo Postgres = {b}/2\n{'#'*72}")
