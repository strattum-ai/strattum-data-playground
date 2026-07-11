"""Migração one-shot: tabelas CLEAN do `.duckdb` → tabelas Delta no lake.

Lê cada tabela de um schema do `strattum.duckdb` e reescreve como tabela Delta
(uma pasta por tabela) usando **delta-rs** (`deltalake.write_deltalake`). É a mesma
lib que o `write_delta` da plataforma já usa. Idempotente: `mode="overwrite"`.

Uso:
    python migrate_duckdb_to_lake.py \
        --duckdb /data/strattum.duckdb \
        --schema main_clean \
        --out    /data/clean            # (ou s3://bucket/clean com AWS_* no ambiente)

Depois: apontar os consumidores (CleanReader, catalog-api) pra `delta_scan('/data/clean/<t>')`.
Ver docs/arquitetura/2.0-lake-aberto/migracao.md.
"""
from __future__ import annotations
import argparse
import duckdb
from deltalake import write_deltalake


def list_tables(con, schema: str) -> list[str]:
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = ? ORDER BY table_name",
        [schema],
    ).fetchall()
    return [r[0] for r in rows]


def migrate(duckdb_path: str, schema: str, out_root: str, storage_options=None) -> dict[str, int]:
    con = duckdb.connect(duckdb_path, read_only=True)   # read-only: nunca corrompe o dbt
    result: dict[str, int] = {}
    try:
        for t in list_tables(con, schema):
            arrow = con.execute(f'SELECT * FROM {schema}."{t}"').fetch_arrow_table()
            write_deltalake(
                f"{out_root}/{t}", arrow,
                mode="overwrite", schema_mode="overwrite",
                storage_options=storage_options,
            )
            result[t] = arrow.num_rows
            print(f"  {schema}.{t:30s} -> {out_root}/{t}  ({arrow.num_rows} linhas)")
    finally:
        con.close()
    return result


def verify(duckdb_path: str, schema: str, out_root: str, migrated: dict[str, int]) -> bool:
    """Confere que a contagem de linhas do Delta bate com a do .duckdb."""
    from deltalake import DeltaTable
    ok = True
    for t, n in migrated.items():
        got = DeltaTable(f"{out_root}/{t}").to_pyarrow_dataset().count_rows()
        flag = "OK" if got == n else "DIVERGE"
        if got != n:
            ok = False
        print(f"  {t:30s} duckdb={n}  delta={got}  [{flag}]")
    return ok


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True)
    ap.add_argument("--schema", default="main_clean")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    print(f"Migrando {args.duckdb} (schema {args.schema}) -> {args.out}")
    migrated = migrate(args.duckdb, args.schema, args.out)
    print(f"\n{len(migrated)} tabela(s) migrada(s). Verificando contagens...")
    ok = verify(args.duckdb, args.schema, args.out, migrated)
    print("\n✅ tudo bate" if ok else "\n🛑 há divergências — investigar antes do cutover")
