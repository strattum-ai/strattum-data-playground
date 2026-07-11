# 04 · Escrita concorrente no DuckDB — resultados

DuckDB é **single-writer**. O que acontece quando vários conectores escrevem no mesmo
banco e como serializar? Notebook: `04_escrita_concorrente.ipynb`. Fecha a Decisão 2 (escrita concorrente) de
[pontos-a-verificar.md](../../docs/arquitetura/2.0-lake-aberto/pontos-a-verificar.md).

> Rodado em **DuckDB 1.5.4**, macOS. Dados isolados em `.data/concurrency/`.

## Como isso aparece na Strattum

Cada conector é um **flow Prefect próprio** (`airtable_sync`, `hubspot_sync`, …) —
processo separado. No fim de cada flow, o `dbt run` roda como **subprocess**
([flows/utils.py](../../strattum-data/services/pipelines/src/flows/utils.py) `_run_dbt`)
e grava os modelos clean no **mesmo arquivo** `strattum.duckdb` (`DBT_DUCKDB_PATH`).

| Camada | Onde grava | Disputa de lock? |
|---|---|---|
| **RAW** | Delta Lake, um path por conector (`/data/raw/{conector}/`) | **não** — arquivos separados |
| **CLEAN** | `strattum.duckdb` **único** (dbt) | **sim** — dois `dbt run` no mesmo arquivo brigam pelo lock |

Como conector = **processo**, o experimento reproduz o caso real com `subprocess` (sem
threads — não é o modelo da Strattum).

## O modelo em uma frase

O lock de escrita do DuckDB é um **file lock por processo**. Dois processos abrindo o
mesmo `.duckdb` em read-write: só **um** pega o lock. Os outros **não esperam** — falham
na hora, já no `connect()`, com `IOException: Could not set lock`.

## O que aconteceu de verdade

| Experimento | Setup | Resultado | Leitura |
|---|---|---|---|
| **A** baseline | 1 writer | `10000` ✅ | sanidade |
| **B** 4 **processos**, mesmo arquivo | disparados juntos, seguram o lock 3s | **1 OK, 3 FALHOU** (`IOException: Could not set lock`) → `10000` | só **1** processo escreve; os outros crasham imediatos, não bloqueiam |
| **Solução** limite = 1 | mesmos 4 processos, **um por vez** | **4 OK** → `40000` ✅ | serializar a escrita resolve; todos gravam |

## A solução: limite de concorrência = 1 (nativo do Prefect)

O Prefect tem **global concurrency limits**. Marca-se o passo de escrita (`dbt run`) com
limite = 1: o Prefect **enfileira** os flows, só um escreve por vez. Extract e transform
seguem em paralelo — só a **escrita** entra no slot.

Uma vez só:

```bash
prefect gcl create duckdb-write --limit 1
```

```python
from prefect.concurrency.sync import concurrency

with concurrency("duckdb-write", occupy=1):   # só um flow escreve por vez
    con = duckdb.connect(DB); con.executemany(...); con.close()
```

## Decisão

- **Default: `global concurrency limit = 1` na tag de escrita.** Custo quase zero,
  nativo do Prefect, e cobre o pior caso — os `dbt run` rodam em **processos** e, sem
  serializar, quebram no lock (exp. B). Escreve serial; o resto do pipeline segue
  concorrente.
- **Alternativa (ingestão pesada): um DuckDB por conector.** Escrita **paralela de
  verdade**, zero disputa; junta-se na leitura com `ATTACH`. Mais complexo e perde o
  "banco único" — só vale se a serialização virar gargalo.
- **Nunca** deixar N processos abrirem o mesmo `.duckdb` em escrita sem serializar: o
  DuckDB **não enfileira**, ele **falha na hora** (exp. B) — vira crash de flow.

Fecha a Decisão 2 (escrita concorrente) de [pontos-a-verificar.md](../../docs/arquitetura/2.0-lake-aberto/pontos-a-verificar.md).
Conecta com o item 1 (RAW+CLEAN: um banco vs. vários) e o item 6 (paralelismo interno
do DuckDB).
