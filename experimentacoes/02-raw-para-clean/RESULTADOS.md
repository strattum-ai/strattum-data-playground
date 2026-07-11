# 02 · RAW → CLEAN — escala e performance do full-refresh (dbt/DuckDB)

O `strattum-data` transforma RAW → CLEAN com **dbt sobre DuckDB**. Medimos se o
full-refresh escala e como acelerar. Notebook: `04_raw_to_clean.ipynb`.

> dbt não tem engine própria — compila SQL pro DuckDB. `dbt run --full-refresh` ≡
> DuckDB rodando `CREATE OR REPLACE TABLE clean.x AS <select>`. É isso que medimos.

## Como medimos

RAW real do folder 01 (`customers_w100m`, 100M × 20 col). Modelo CLEAN com
normalizações (`LOWER/TRIM`, `external_id`, UTC, `CASE`), `LEFT JOIN` com dim de
1.000 linhas e uma **window `rank()`** (de stress). Cada run em processo isolado,
medições a frio.

## Resultados (8 cores, 8 GB RAM, SSD)

**Escala (threads=8):**

| Linhas | tempo | RSS | spill |
|---|---|---|---|
| 1M | 2,9 s | 857 MB | não |
| 10M | 23,3 s | 2,2 GB | não |
| 100M | **812 s (~13,5 min)** | 3,2 GB | **sim, pesado** |

**Cores (10M):**

| threads | tempo | speedup |
|---|---|---|
| 1 | 93,9 s | 1,00× |
| 4 | 26,8 s | 3,51× |
| 8 | 22,4 s | 4,19× |

**O que significa:**

1. **Escala ~linear até caber na RAM.** Super-linear no 100M porque a `rank()`
   ordena 100M, estoura a RAM e derrama pra disco (I/O de spill domina).
2. **RAM nunca estoura — out-of-core.** RSS só 3,2 GB no 100M, sem OOM.
3. **Cores saturam após 4 threads** (query de sort limitada por banda de memória).
   `threads: 4` da prod é sweet spot.

## Padrão real

- Clean real = **42 models dbt** (`materialized='table'`, full-refresh, lendo RAW
  via `delta_scan` + `LEFT JOIN`). Nosso modelo bate nisso; a `rank()` é stress
  (nenhum dos 42 usa window).
- Prefect roda `dbt run --select clean.<modelos>` por subprocess. Profile:
  `threads: 4`, sem `memory_limit`.

## ✅ Decisão

**Manter dbt + DuckDB para RAW→CLEAN.** Escala out-of-core (100M com RAM plana) e
entrega SQL versionado, testes, lineage — sem cluster.

- **`threads` = nº de cores** (4 já é sweet spot).
- Em volume grande com spill: setar `memory_limit` + `temp_directory` em NVMe.
- **Próximo ganho é o incremental** (folder 03), não tooling.
- **Formato do RAW (decisão "tudo em DuckDB"):** pontos acoplados ao Delta e
  consequências registrados em
  [docs/arquitetura/2.0-lake-aberto/pontos-a-verificar.md](../../docs/arquitetura/2.0-lake-aberto/pontos-a-verificar.md).
