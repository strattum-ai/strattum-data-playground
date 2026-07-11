# Experimentações — Strattum

Espaço para testar partes da arquitetura de dados fora dos serviços, em notebooks.
Organizado **por objetivo**, numerado e contíguo. Cada folder tem um notebook (enxuto,
sem outputs), um `RESULTADOS.md` e os scripts reutilizáveis.

Complementa os docs de arquitetura ([`docs/arquitetura/`](../docs/arquitetura/)) — cada
`RESULTADOS.md` alimenta uma **descoberta** ou um **ponto a verificar** da
[arquitetura 2.0](../docs/arquitetura/2.0-lake-aberto/).

## Estrutura

| Pasta | Objetivo | Alimenta |
|---|---|---|
| [`01-ingestao-fonte-para-raw/`](01-ingestao-fonte-para-raw/) | Fonte (Postgres) → RAW: caseiro vs batch vs dlt vs **dlt+connectorx**. Ponto de quebra (OOM) e escala a 100M. | ✅ decisão: dlt + connectorx |
| [`02-raw-para-clean/`](02-raw-para-clean/) | Transform RAW → CLEAN (dbt/DuckDB) — escala? satura em quantas threads? | 📊 out-of-core, satura em ~4 threads |
| [`03-incremental/`](03-incremental/) | Sync incremental + bug do watermark (sem `updated_at` → full scan) | ✅ incremental 8×; T2/T5 |
| [`04-escrita-concorrente/`](04-escrita-concorrente/) | DuckDB é single-writer — como serializar N conectores escrevendo | ✅ concurrency limit = 1 (T3) |
| [`05-formato-storage-lake/`](05-formato-storage-lake/) | **`.duckdb` vs DuckLake vs Delta** — MinIO, pipeline inteiro nos dois, dbt escreve Delta | 🛑 decisão de storage (aberto §1) |
| [`06-federation-read-engine/`](06-federation-read-engine/) | Ler a CLEAN do cliente **direto → grafo**, sem ETL — **DuckDB vs Trino** | 🛑 federation (aberto §2) |
| [`07-federation-dbt-clean/`](07-federation-dbt-clean/) | **dbt** juntando RAW (DuckLake) + fonte **federada (Trino)** → **CLEAN nova na DuckLake** | ✅ federation na escrita funciona |
| [`08-federation-adbc-databricks/`](08-federation-adbc-databricks/) | Federação via **extensão ADBC do DuckDB** — lê Databricks/Snowflake **direto**, sem Trino/plugin | ✅ 3ª via de federação (ADBC) |
| [`09-ducklake-concorrencia/`](09-ducklake-concorrencia/) | **N conectores escrevendo juntos** no DuckLake — catálogo `.duckdb` vs **Postgres**, dados no MinIO | ✅ catálogo Postgres = escrita concorrente |

> Cada pasta tem seu `README.md`. A camada de dados (`.data/`) fica na raiz de
> `experimentacoes` (os notebooks fazem `os.chdir` pra cá na 1ª célula via `exp.py`).

## Setup (uma vez)

> ⚠️ **Requer Python 3.10+** (a plataforma roda 3.12). Notebooks importam o
> `strattum_core`, que usa sintaxe de tipos do 3.10+.

```bash
cd experimentacoes
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m ipykernel install --user --name strattum-exp --display-name "Python (exp)"
```

Depois abra qualquer notebook e selecione o kernel **"Python (exp)"**.

## Antes de rodar

1. Tenha um Postgres com dados (segue o `local-ingestion-runbook.md`).
2. Edite a célula de **Configuração** (`PG_DSN`, `TABLE`, `MODE`), ou exporte `EXP_PG_DSN`.
3. Notebooks de Trino/FalkorDB sobem containers Docker — rode fora de janelas de benchmark.

> `.data/`, `.venv/`, `.out/` e `__pycache__` são **artefatos gerados** e ficam fora do
> git (ver `.gitignore`). A pasta inteira `experimentacoes/` é ignorada no repo raiz.
