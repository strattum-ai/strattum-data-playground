# 09 · DuckLake — escrita concorrente e backend de catálogo

**Objetivo:** responder se **N conectores** conseguem escrever no **mesmo DuckLake ao
mesmo tempo** na RAW — e o quanto isso depende do **backend do catálogo** (arquivo
`.duckdb` vs **Postgres**). Fecha o ponto aberto do [`04`](../04-escrita-concorrente/)
(*DuckDB é single-writer → concurrency limit = 1*) no contexto DuckLake.

Dois conectores, **processos separados**, escrevem juntos:
- conectorA lê **MySQL** (`srcdb.customers`) → `lake.raw_customers_mysql`
- conectorB lê **Postgres** (`src.orders`) → `lake.raw_orders_pg`

Dados vão pro **MinIO** (`s3://`), não pro disco.

- Teste: [`concurrency_test.py`](concurrency_test.py) — 2 processos, os 2 backends de catálogo.
- Radiografia do catálogo: [`inspect_catalog.py`](inspect_catalog.py) — as 22 tabelas `ducklake_*`.
- Achados e caveats: [`RESULTADOS.md`](RESULTADOS.md)

## Setup dos containers

```bash
# MinIO (S3) — dados do lake
docker run -d --name exp-minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:latest server /data --console-address ":9001"
# bucket 'lake' (via boto3 no venv) + MySQL fonte
docker run -d --name exp-mysql -p 3307:3306 -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=srcdb mysql:8
# Postgres: já é o strattum-postgres (:5432); criar DB do catálogo:
#   CREATE DATABASE ducklake_catalog;
```

> Pré-req: `exp-minio` (:9000) com bucket `lake`, `exp-mysql` (:3307) com
> `srcdb.customers`, `strattum-postgres` (:5432) com `demo_source.src.orders` e o
> DB `ducklake_catalog`. As fontes são semeadas no setup (ver `RESULTADOS.md`).
