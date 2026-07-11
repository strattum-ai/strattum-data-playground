# Ingestão Fonte → RAW — Resultados

## Problema

Os conectores extraem em streaming (`yield` registro a registro), mas o flow
**acumula a tabela inteira numa lista Python** antes de gravar
(`records.append(...)` → `write_delta(records)`). Precisávamos medir em que
volume essa lista estoura a memória e qual abordagem usar.

## Como medimos

Mesma tabela, escala crescente, várias abordagens. Duas métricas de memória:

- **Heap** — memória que o código Python segura (a tal lista). É o que estoura hoje.
- **Pico de RAM (RSS)** — RAM física total, incluindo buffers Arrow em C
  (Polars/connectorx).

> Heap diz se o código é gastão; RAM diz se cabe na máquina. Teto do worker: **512 MB**.

## Resultados

**Leitura serial (1 conexão):**

| Volume | caseiro (atual) | batch 50k | dlt → DuckDB (pyarrow) |
|---|---|---|---|
| 1M   | 76s  | 65s  | 16s   |
| 5M   | 503s | 310s | 61s   |
| 10M  | 🛑 quebra | 711s | 120s |
| 100M | 🛑 | ~8–10h (inviável) | ~25min |

**Leitura paralela (connectorx, 8 conexões, tabela larga de 20 colunas):**

| Volume | dlt + connectorx → DuckDB | Polars → Delta |
|---|---|---|
| 5M   | 14.6s · heap 49 MB · RAM 1.080 MB | 12.7s · heap 9 MB · RAM 1.277 MB |
| 10M  | 22.0s · heap 49 MB · RAM 1.378 MB | 23.1s · heap 9 MB · RAM 1.490 MB |
| 100M | **212s** · RAM **1.415 MB** | 🛑 OOM (1 chamada) · 573s em chunks |

- **dlt+connectorx segura a RAM plana** (streama em batches pro disco, não cresce com a tabela).
- **Polars puro quebra no 100M** (carrega o DataFrame inteiro → OOM-kill).
- **Polars só escala em chunks manuais** — ~2,5× mais lento que dlt+connectorx.

**Caseiro (jeito atual) — heap cresce com a tabela:** 982 MB no 1M, 4.754 MB no
5M → estoura no worker de 512 MB por volta de 1–2M linhas.

## Decisão

- ✅ **Padrão: `dlt → DuckDB` com leitura `connectorx`.** Mais rápido, RAM plana,
  ganha schema evolution + retomada de graça. A mudança no código é só trocar o
  backend de leitura de `pyarrow` para `connectorx`.

> **Regra:** leitura sempre paralela (connectorx) sobre **coluna indexada** — sem
> índice, o particionamento vira seq scan. Escolha pelo formato de saída: DuckDB →
> dlt+connectorx.
