"""08 · Federation via ADBC — DuckDB lê uma fonte externa DIRETO (sem Trino, sem plugin).

A extensão `adbc` (community, query-farm/adbc_scanner) faz o DuckDB virar CLIENTE ADBC:
`read_adbc('<uri>', '<query>')` conecta em qualquer sistema com driver ADBC e traz os
dados em Arrow (zero-copy). Aqui a fonte é Postgres (faz o papel do Databricks/Snowflake);
trocar pra Databricks = trocar o driver + a URI (ver RESULTADOS.md).

Fluxo:
    Postgres (fed.products)  --read_adbc-->  DuckDB  --JOIN com--  DuckLake (orders)  -->  CLEAN

Pré-req: Postgres do starter no ar (:5432). Drivers/extensão instalados por este script.
Rodar:  ../.venv/bin/python federate_adbc.py
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

sys.path.insert(0, "/Users/allanfraga/Repos/strattum/experimentacoes")
import exp  # noqa: E402
import duckdb  # noqa: E402

HERE = Path(__file__).parent.resolve()
OUT = HERE / ".out"
LAKE_CAT = OUT / "lake" / "catalog.ducklake"
LAKE_DATA = OUT / "lake" / "data"
DRV_DIR = OUT / "adbc_drivers"


def adbc_postgres_manifest() -> None:
    """Registra o driver ADBC de Postgres via manifesto (ADBC_DRIVER_PATH).

    O scheme da URI ('postgresql') vira o nome do driver, resolvido por
    <nome>.toml. Pra Databricks: manifesto databricks.toml apontando pro
    libadbc_driver_databricks (adbc-drivers.org).
    """
    import adbc_driver_postgresql

    so = Path(adbc_driver_postgresql.__file__).parent / "libadbc_driver_postgresql.so"
    DRV_DIR.mkdir(parents=True, exist_ok=True)
    (DRV_DIR / "postgresql.toml").write_text(
        "manifest_version = 1\nname = 'PostgreSQL Driver'\nversion = '1.0.0'\n"
        f"[ADBC]\nversion = '1.1.0'\n[Driver]\nshared = '{so}'\n"
    )
    os.environ["ADBC_DRIVER_PATH"] = str(DRV_DIR)


def seed_federated_source() -> None:
    """Fonte federada no Postgres (papel do lake do cliente)."""
    con = duckdb.connect()
    pg = exp.attach_postgres(con, alias="pg", db="demo_source", read_only=False)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {pg}.fed")
    con.execute(f"DROP TABLE IF EXISTS {pg}.fed.products")
    con.execute(
        f"""CREATE TABLE {pg}.fed.products AS SELECT * FROM (VALUES
        ('SKU-001','Bomba Centrifuga 5cv','Hidraulica',4200.00),
        ('SKU-002','Valvula Esfera 2pol','Conexoes',180.50),
        ('SKU-003','Motor Trifasico 10cv','Eletrica',3100.00),
        ('SKU-004','Inversor de Frequencia','Eletrica',2750.75),
        ('SKU-005','Sensor de Pressao','Instrumentacao',640.00)
    ) t(sku, product_name, category, unit_price)"""
    )
    con.close()


def build_our_raw() -> None:
    """Nossa RAW em DuckLake (orders)."""
    shutil.rmtree(OUT / "lake", ignore_errors=True)
    LAKE_DATA.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake")
    con.execute(f"ATTACH 'ducklake:{LAKE_CAT}' AS lake (DATA_PATH '{LAKE_DATA}/')")
    con.execute(
        """CREATE TABLE lake.orders AS SELECT * FROM (VALUES
        (1,'SKU-001','ana@acme.com',2),(2,'SKU-003','ana@acme.com',1),
        (3,'SKU-002','joao@beta.com',10),(4,'SKU-005','joao@beta.com',4),
        (5,'SKU-004','maria@gama.com',1)) t(order_id, product_sku, email, qty)"""
    )
    con.execute("DETACH lake")
    con.close()


def main() -> None:
    adbc_postgres_manifest()
    seed_federated_source()
    build_our_raw()
    dsn = exp.dsn("demo_source")

    con = duckdb.connect()
    con.execute("INSTALL adbc FROM community; LOAD adbc;")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH 'ducklake:{LAKE_CAT}' AS lake (DATA_PATH '{LAKE_DATA}/', READ_ONLY)")

    # federação: DuckDB lê o Postgres DIRETO via ADBC (Arrow), sem Trino/plugin
    print("read_adbc — products lidos direto do Postgres via ADBC:")
    print(con.sql(f"SELECT * FROM read_adbc('{dsn}', 'SELECT sku, product_name, unit_price FROM fed.products') LIMIT 3"))

    # JOIN: nossa RAW (DuckLake) x fonte federada (ADBC) — tudo no DuckDB
    res = con.sql(f"""
        SELECT o.order_id, o.email, p.product_name, p.category,
               o.qty, CAST(p.unit_price AS DECIMAL(10,2)) AS unit_price,
               o.qty * CAST(p.unit_price AS DECIMAL(10,2)) AS line_total
        FROM lake.orders o
        JOIN read_adbc('{dsn}',
            'SELECT sku, product_name, category, unit_price FROM fed.products') p
          ON o.product_sku = p.sku
        ORDER BY o.order_id""")
    print("\nJOIN  DuckLake(orders)  x  Postgres-ADBC(products)  — engine=DuckDB, sem Trino, sem plugin:")
    print(res)
    print("\n✅ Federation via ADBC OK. Pra Databricks: trocar o driver (databricks.toml) e a URI 'databricks://...'.")
    con.close()


if __name__ == "__main__":
    main()
