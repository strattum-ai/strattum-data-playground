"""09 · Radiografia do catálogo DuckLake (no Postgres).

Lista as tabelas de metadados `ducklake_*`, o que cada uma guarda, e amostra as
principais. Rode DEPOIS de concurrency_test.py (usa o DB `ducklake_catalog`).
Rodar:  ../.venv/bin/python inspect_catalog.py
"""
from __future__ import annotations
import duckdb

PWPG = next(l.split("=", 1)[1].strip() for l in
            open("/Users/allanfraga/Repos/strattum/strattum-deploy/starter/.env")
            if l.startswith("POSTGRES_PASSWORD="))

# o que cada tabela do catálogo guarda (schema DuckLake)
DOC = {
    "ducklake_metadata": "config global do lake (versão do formato, flags).",
    "ducklake_snapshot": "cada COMMIT = 1 snapshot (versão) do lake — base do time-travel.",
    "ducklake_snapshot_changes": "o que mudou em cada snapshot (create_table, insert...).",
    "ducklake_schema": "schemas (namespaces) do lake, ex: main.",
    "ducklake_schema_versions": "qual versão de schema vale em cada snapshot.",
    "ducklake_table": "tabelas registradas (id, nome, schema, vida por snapshot).",
    "ducklake_view": "views registradas (nenhuma aqui).",
    "ducklake_column": "o SCHEMA de cada tabela: coluna, tipo, ordem, default, nullable.",
    "ducklake_column_mapping": "mapeamento de ids de coluna (schema evolution).",
    "ducklake_name_mapping": "mapeia nomes de coluna do parquet -> field ids.",
    "ducklake_column_tag": "tags/props por coluna.",
    "ducklake_tag": "tags/props por objeto.",
    "ducklake_data_file": "MAPA tabela -> arquivo(s) parquet no storage (+ record_count, tamanho).",
    "ducklake_delete_file": "delete files (deletes posicionais p/ merge-on-read).",
    "ducklake_files_scheduled_for_deletion": "parquets órfãos aguardando GC/vacuum.",
    "ducklake_inlined_data_tables": "dados pequenos inline no catálogo (sem parquet).",
    "ducklake_file_column_stats": "min/max/null por coluna POR ARQUIVO — pruning na leitura.",
    "ducklake_table_column_stats": "min/max/null por coluna agregado por TABELA.",
    "ducklake_table_stats": "stats por tabela (record_count, próximo row_id, bytes).",
    "ducklake_partition_info": "definição de particionamento por tabela.",
    "ducklake_partition_column": "colunas de partição + transform.",
    "ducklake_file_partition_value": "valor de partição de cada arquivo.",
}


def main() -> None:
    c = duckdb.connect(); c.execute("INSTALL postgres; LOAD postgres;")
    c.execute(f"ATTACH 'host=localhost port=5432 dbname=ducklake_catalog user=strattum password={PWPG}' AS m (TYPE postgres, READ_ONLY)")
    tbls = [r[0] for r in c.execute("SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog='m' AND table_name LIKE 'ducklake_%' ORDER BY 1").fetchall()]
    print(f"CATÁLOGO DuckLake no Postgres — {len(tbls)} tabelas de metadados:\n")
    for t in tbls:
        n = c.execute(f"SELECT count(*) FROM m.{t}").fetchone()[0]
        print(f"  {t:<38} {n:>3} linha(s)  — {DOC.get(t, '')}")

    print("\n── amostras das principais ──")
    for title, sql in [
        ("ducklake_snapshot (versões do lake)",
         "SELECT snapshot_id, snapshot_time, schema_version FROM m.ducklake_snapshot ORDER BY 1"),
        ("ducklake_table (tabelas)",
         "SELECT table_id, table_name, schema_id FROM m.ducklake_table ORDER BY 1"),
        ("ducklake_data_file (tabela -> parquet no MinIO)",
         "SELECT table_id, regexp_extract(path,'[^/]+$') AS parquet, record_count, file_size_bytes FROM m.ducklake_data_file ORDER BY 1"),
        ("ducklake_column (schema das tabelas)",
         "SELECT table_id, column_name, column_type FROM m.ducklake_column ORDER BY table_id, column_order"),
        ("ducklake_snapshot_changes (o que cada commit fez)",
         "SELECT snapshot_id, changes_made FROM m.ducklake_snapshot_changes ORDER BY 1"),
    ]:
        print(f"\n• {title}")
        print(c.sql(sql))
    c.close()


if __name__ == "__main__":
    main()
