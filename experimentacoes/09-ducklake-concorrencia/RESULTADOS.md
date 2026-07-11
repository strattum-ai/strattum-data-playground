# 09 · DuckLake — escrita concorrente & backend de catálogo — resultados

> Rodado em **DuckDB 1.4.5 · ducklake · httpfs · postgres · mysql** · MinIO (S3) ·
> MySQL 8 · Postgres 16. Dois conectores em **processos separados**.
> Scripts: [`concurrency_test.py`](concurrency_test.py), [`inspect_catalog.py`](inspect_catalog.py).

## A pergunta

Dois conectores (um lendo **MySQL**, outro **Postgres**) conseguem escrever no **mesmo
DuckLake ao mesmo tempo**? E isso muda conforme o **catálogo de metadados** seja um
arquivo `.duckdb` ou um **Postgres**? Dados no **MinIO**.

## O que aconteceu

| Cenário | Catálogo | Dados | Conectores concorrentes |
|---|---|---|---|
| **A** | `.duckdb` (arquivo) | MinIO `s3://` | **1/2** ❌ — o 2º falha no `ATTACH` (lock do arquivo) |
| **B** | **Postgres** | MinIO `s3://` | **2/2** ✅ — os dois commitam |

Erro do cenário A (o 2º conector):
```
IOException: Failed to attach DuckLake MetaData "__ducklake_metadata_lake" … (lock)
```
No cenário B, o `ducklake_snapshot_changes` registra os **dois** commits:
```
snapshot 2: created_table "main"."raw_customers_mysql"   (conectorA, do MySQL)
snapshot 3: created_table "main"."raw_orders_pg"          (conectorB, do Postgres)
```
Os dados foram pro **MinIO** (`s3://lake/scenarioB/main/<tabela>/*.parquet`), confirmado
listando o bucket. O catálogo (22 tabelas `ducklake_*`) ficou no Postgres.

## Conclusão

- 🛑 **Catálogo-arquivo `.duckdb` = single-writer.** O arquivo é aberto com **lock
  exclusivo**; um 2º processo escrevendo no mesmo lake falha já no `ATTACH`. É o limite
  do [`04`](../04-escrita-concorrente/) reaparecendo — com N conectores, **não serve**.
- ✅ **Catálogo Postgres = escrita concorrente de verdade.** Cada conector commita numa
  transação própria no Postgres (snapshot isolation do DuckLake); os dois criaram suas
  tabelas raw **ao mesmo tempo**, dados no MinIO. É o caminho pra **N conectores**.
- 🧱 **Separação limpa:** metadados no **Postgres** (o "cérebro": tabelas, schema,
  snapshots, mapa dos parquets), dados em **parquet no MinIO**. A engine (DuckDB) é
  descartável/stateless.

## ⚠️ Nuance importante: inicialização é corrida

Rodar os 2 conectores contra um catálogo Postgres **vazio** falha (1/2): os dois tentam
**criar as tabelas `ducklake_*` ao mesmo tempo** (`CREATE TABLE ducklake_metadata` colide).
Solução: **inicializar o lake UMA vez** (bootstrap) antes de apontar N writers. Com o lake
já criado, a escrita concorrente passa (2/2). No fluxo real, o lake é provisionado no
setup do cliente, não a cada carga.

## Caveats (a cravar)

- **Mesma tabela, 2 writers → concorrência otimística:** pode dar conflito de commit e
  exigir **retry**. Tabelas distintas (1 conector = 1 tabela raw) não conflitam — foi o
  caso testado. Definir a política (retry/backoff) por conector.
- **O Postgres do catálogo vira dependência crítica** (é o cérebro do lake) → backup/HA.
- **Storage compartilhado obrigatório** pra writers distribuídos: `DATA_PATH` no MinIO/S3
  (testado), não disco local.
- **`ATTACH` do catálogo Postgres precisa da extensão `postgres`** carregada antes.
- Não medimos **throughput** sob muitos writers nem GC/`vacuum` de parquets órfãos
  (tabela `ducklake_files_scheduled_for_deletion`) — em aberto.

## As 22 tabelas do catálogo (o que cada uma guarda)

| Tabela | Guarda |
|---|---|
| `ducklake_metadata` | config global do lake (versão do formato, flags) |
| `ducklake_snapshot` | cada COMMIT = 1 snapshot (versão) — base do time-travel |
| `ducklake_snapshot_changes` | o que mudou em cada snapshot (create_table, insert…) |
| `ducklake_schema` / `ducklake_schema_versions` | schemas (namespaces) e sua versão por snapshot |
| `ducklake_table` | tabelas registradas (id, nome, schema, vida por snapshot) |
| `ducklake_view` | views registradas |
| `ducklake_column` | **o schema de cada tabela** (coluna, tipo, ordem, default, nullable) |
| `ducklake_column_mapping` / `ducklake_name_mapping` | ids de coluna e mapa nome↔field (schema evolution) |
| `ducklake_column_tag` / `ducklake_tag` | tags/props por coluna/objeto |
| `ducklake_data_file` | **mapa tabela → parquet** no storage (+ record_count, tamanho) |
| `ducklake_delete_file` | delete files (merge-on-read) |
| `ducklake_files_scheduled_for_deletion` | parquets órfãos aguardando GC/vacuum |
| `ducklake_inlined_data_tables` | dados pequenos inline no catálogo (sem parquet) |
| `ducklake_file_column_stats` | min/max/null por coluna **por arquivo** — pruning |
| `ducklake_table_column_stats` | min/max/null por coluna agregado **por tabela** |
| `ducklake_table_stats` | stats por tabela (record_count, próximo row_id, bytes) |
| `ducklake_partition_info` / `ducklake_partition_column` / `ducklake_file_partition_value` | particionamento (def, colunas+transform, valor por arquivo) |

> Alimenta a decisão de storage/catálogo da [arquitetura 2.0](../../docs/arquitetura/2.0-lake-aberto/)
> e resolve o ponto de concorrência do [`04`](../04-escrita-concorrente/).
