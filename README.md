# strattum-data-playground

Experiments and reference documents for the Strattum open lakehouse
(DuckLake + Parquet/S3 → dbt clean → FalkorDB graph).

| Document | What it is |
|---|---|
| [ARQUITETURA-LAKEHOUSE.md](ARQUITETURA-LAKEHOUSE.md) ([PDF](ARQUITETURA-LAKEHOUSE.pdf)) | Reference architecture: connectors → raw → clean → graph, LakeStore, federation, ACL |
| [BENCHMARK-LAKEHOUSE.md](BENCHMARK-LAKEHOUSE.md) ([PDF](BENCHMARK-LAKEHOUSE.pdf)) | End-to-end benchmark: 3M rows (PostgreSQL + MongoDB) → lake → clean → graph, on an 8 GB laptop |
| [experimentacoes/](experimentacoes/) | Numbered experiments (ingestion, incremental, concurrency, federation, enrichment) |
| [documents/arquitetura/](documents/arquitetura/) | Working notes: architecture 1.0 (current) and 2.0 (open lake) decision ledger |
