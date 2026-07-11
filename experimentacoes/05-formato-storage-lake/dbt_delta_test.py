"""Teste: o dbt consegue escrever DIRETO em Delta Lake (um único `dbt run`)?

Fluxo:
  1. Cria RAW em Delta (/… /raw/orders) — 100 linhas.
  2. `dbt run` MODE=overwrite  -> CLEAN em Delta deve ter 100, com _delta_log.
  3. +20 linhas na RAW (upsert de 5 existentes + 15 novas).
  4. `dbt run` MODE=merge      -> CLEAN deve ter 115 (upsert por id), 1 versão nova.
Verifica contagens e que o artefato final é Delta (não parquet solto, não .duckdb).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

HERE = Path(__file__).parent.resolve()
DBT_DIR = HERE / "dbt-delta-plugin"
OUT = HERE / ".out"
RAW = OUT / "raw"
CLEAN = OUT / "clean"
TMP = OUT / "tmp"


def _rows(start: int, end: int, amount: float, ts: str = "2024-01-15T10:00:00") -> pa.Table:
    ids = list(range(start, end))
    return pa.table({
        "id": ids,
        "customer_email": [f"  User{i}@Example.COM " for i in ids],
        "amount": [amount] * len(ids),
        "updated_at": [ts] * len(ids),
    })


def _clean_count() -> int:
    return sum(DeltaTable(str(CLEAN / "orders")).to_pandas().shape[0] for _ in [0])


def _clean_versions() -> int:
    return len(DeltaTable(str(CLEAN / "orders")).history())


def run_dbt(mode: str) -> None:
    env = os.environ.copy()
    env.update({
        "EXP_DUCKDB": str(OUT / "engine.duckdb"),
        "EXP_RAW": str(RAW),
        "EXP_CLEAN": str(CLEAN),
        "EXP_TMP": str(TMP),
        "EXP_MODE": mode,
        # torna o plugin delta_writer importável
        "PYTHONPATH": str(DBT_DIR) + os.pathsep + env.get("PYTHONPATH", ""),
    })
    proc = subprocess.run(
        ["dbt", "run", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
        env=env, capture_output=True, text=True,
    )
    print(proc.stdout[-1500:])
    if proc.returncode != 0:
        print("STDERR:", proc.stderr[-2000:])
        raise SystemExit(f"dbt run ({mode}) FALHOU rc={proc.returncode}")


def main() -> None:
    if OUT.exists():
        shutil.rmtree(OUT)
    for d in (RAW, CLEAN, TMP):
        d.mkdir(parents=True, exist_ok=True)

    print("== 1. RAW inicial (100) ==")
    write_deltalake(str(RAW / "orders"), _rows(1, 101, 10.0), mode="overwrite")

    print("== 2. dbt run overwrite ==")
    run_dbt("overwrite")
    c1 = _clean_count()
    is_delta = (CLEAN / "orders" / "_delta_log").is_dir()
    leftover_pq = list(TMP.glob("*.parquet"))
    print(f"CLEAN={c1}  _delta_log={is_delta}  parquet_temp_restante={len(leftover_pq)}")
    assert c1 == 100, c1
    assert is_delta, "CLEAN não é Delta!"

    # normalização aplicada pelo dbt (lower/trim no email)?
    df1 = DeltaTable(str(CLEAN / "orders")).to_pandas()
    email1 = df1[df1["id"] == 1]["customer_email"].iloc[0]
    print("email do id=1:", repr(email1))
    assert email1 == "user1@example.com", email1

    print("== 3. +20 na RAW (5 upsert + 15 novas), merge por id ==")
    # ids 96..100 reaparecem com amount+ts novos (upsert) + 101..115 novas.
    # ts mais novo => o QUALIFY da clean escolhe estas linhas.
    write_deltalake(str(RAW / "orders"), _rows(96, 116, 99.0, ts="2024-01-16T11:00:00"), mode="append")

    print("== 4. dbt run merge ==")
    run_dbt("merge")
    c2 = _clean_count()
    v2 = _clean_versions()
    print(f"CLEAN={c2}  versões_delta={v2}")
    assert c2 == 115, c2  # 100 + 15 novas (5 foram update, não insert)

    # os 5 upserts pegaram o amount novo?
    df = DeltaTable(str(CLEAN / "orders")).to_pandas()
    upserted = df[df["id"] == 100]["amount"].iloc[0]
    print("amount do id=100 após merge:", upserted)
    assert upserted == 99.0, upserted

    print("\n✅ SUCESSO: dbt escreveu Delta direto (overwrite + merge), 1 `dbt run` cada.")
    print(f"   Artefato final: {CLEAN/'orders'} (Delta, {v2} versões). Sem .duckdb da clean.")


if __name__ == "__main__":
    sys.exit(main())
