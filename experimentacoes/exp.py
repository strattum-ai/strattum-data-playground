"""Setup compartilhado dos experimentos.

Uso no topo de qualquer notebook (2 linhas):
    import sys; sys.path.insert(0, "/Users/allanfraga/Repos/strattum/experimentacoes")
    import exp

Seções:
    1. Paths      — REPO, EXP, DATA, DUCKDB (+ sys.path da plataforma)
    2. Conexão    — dsn(), attach_postgres()
    3. Medição    — rss_mb(), measure()
    4. Contagem   — count_delta(), count_duckdb()
    5. dlt        — reset_dlt(), dlt_pipeline()
"""
import os
import sys
import time
import resource
import tracemalloc


# ── 1. Paths ──────────────────────────────────────────────────────────────────
REPO   = "/Users/allanfraga/Repos/strattum"
EXP    = f"{REPO}/experimentacoes"
DATA   = f"{EXP}/.data"               # camada de dados compartilhada
DUCKDB = f"{DATA}/dlt.duckdb"         # DuckDB compartilhado onde o dlt grava

os.chdir(EXP)                         # .data compartilhado na raiz de experimentacoes
for _p in (f"{REPO}/strattum-data/services/pipelines/src", f"{REPO}/strattum-core/src"):
    if _p not in sys.path:
        sys.path.insert(0, _p)        # importar o código real da plataforma


# ── 2. Conexão ────────────────────────────────────────────────────────────────
def dsn(db: str = "demo_source") -> str:
    """DSN do Postgres local (lê a senha do .env do starter)."""
    pw = next(l.split("=", 1)[1].strip()
              for l in open(f"{REPO}/strattum-deploy/starter/.env")
              if l.startswith("POSTGRES_PASSWORD="))
    return f"postgresql://strattum:{pw}@localhost:5432/{db}"


def attach_postgres(con, alias: str = "pg", db: str = "demo_source", read_only: bool = True):
    """ATTACH do Postgres no DuckDB a partir do DSN — sem montar a string na mão."""
    from urllib.parse import urlparse
    u = urlparse(dsn(db))
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(
        f"ATTACH 'host={u.hostname} port={u.port} dbname={u.path.lstrip('/')} "
        f"user={u.username} password={u.password}' AS {alias} "
        f"(TYPE postgres{', READ_ONLY' if read_only else ''})"
    )
    return alias


# ── 3. Medição ────────────────────────────────────────────────────────────────
def rss_mb() -> float:
    """RSS pico do processo em MB (macOS bytes / Linux KB)."""
    v = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return v / 1e6 if v > 1e6 else v / 1e3


class measure:
    """Context manager que mede tempo + heap Python + RSS pico.

        with exp.measure("dlt 1M"):
            ...
    """
    def __init__(self, label: str):
        self.label = label

    def __enter__(self):
        tracemalloc.start()
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *_):
        el = time.perf_counter() - self.t0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(f"{self.label}: {el:.1f}s · heap {peak/1e6:.0f}MB · rss {rss_mb():.0f}MB")


# ── 4. Contagem ───────────────────────────────────────────────────────────────
def count_delta(path: str) -> int:
    from deltalake import DeltaTable
    return DeltaTable(path).to_pyarrow_dataset().count_rows()


def count_duckdb(dbfile: str, table: str) -> int:
    import duckdb
    return duckdb.connect(dbfile, read_only=True).sql(f"SELECT count(*) FROM {table}").fetchone()[0]


# ── 5. dlt ────────────────────────────────────────────────────────────────────
def reset_dlt(name: str = "exp") -> None:
    """Limpa o estado de um pipeline dlt (evita load packages pendentes de runs mortos)."""
    import shutil
    shutil.rmtree(os.path.expanduser(f"~/.dlt/pipelines/{name}"), ignore_errors=True)


def dlt_pipeline(name: str = "exp", dataset: str = "postgres"):
    """Pipeline dlt limpo, gravando no DuckDB compartilhado (exp.DUCKDB)."""
    import dlt
    reset_dlt(name)
    return dlt.pipeline(name, destination=dlt.destinations.duckdb(DUCKDB), dataset_name=dataset)
