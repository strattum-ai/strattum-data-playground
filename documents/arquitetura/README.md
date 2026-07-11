# Documentação de arquitetura — Strattum

Organizada **por versão da arquitetura**. Cada versão diz o que roda e o que muda; a 2.0
carrega suas descobertas, o que falta verificar e o backlog de tarefas.

```
pergunta aberta ──investiga──► descoberta ──► tarefa ──► vira arquitetura atual
(pontos-a-verificar)          (descobertas)  (tarefas/)   (1.0-atual)
```

## Mapa

| Documento | O que é |
|---|---|
| **[1.0-atual.md](1.0-atual.md)** | Como o pipeline roda **hoje**, validado (fonte → RAW → CLEAN `.duckdb` → grafo) e **onde quebra** em escala |
| **[2.0-lake-aberto/](2.0-lake-aberto/)** | A arquitetura-**alvo**: lake aberto (DuckLake/Delta) em object storage + federation. Ver abaixo ↓ |

### Dentro de [2.0-lake-aberto/](2.0-lake-aberto/)

| Documento | O que vai aqui |
|---|---|
| **[README](2.0-lake-aberto/README.md)** | A visão-alvo, o diagrama (Hoje → Futuro) e o que já está **decidido vs em aberto** |
| **[descobertas.md](2.0-lake-aberto/descobertas.md)** | O que já foi **testado/concluído** (findings dos experimentos + decisões) |
| **[pontos-a-verificar.md](2.0-lake-aberto/pontos-a-verificar.md)** | O que ainda está **aberto** (perf do grafo, deep-dive por conector, grafo em container separado) |
| **[tarefas/](2.0-lake-aberto/tarefas/)** | **Backlog em pastas**, uma por iniciativa (lakehouse · conectores · federation · data quality) |
| **[migracao.md](2.0-lake-aberto/tarefas/01-lakehouse/migracao.md)** | Plano de migração da CLEAN pro lake — **agnóstico Delta/DuckLake** — código (pipeline, catalog-api, memory-worker) + runbook |

### Diagramas

| Arquivo | Assunto |
|---|---|
| [diagramas/1.0-containers.svg](diagramas/1.0-containers.svg) | Containers de hoje (quem roda o quê no docker-compose) |
| [diagramas/2.0-visao-geral.svg](diagramas/2.0-visao-geral.svg) | Fluxo: hoje · ingestão (lake aberto) · federation |

> Os experimentos ficam em [`experimentacoes/`](../../experimentacoes/) (um folder por
> investigação, com `RESULTADOS.md`). Cada `RESULTADOS.md` alimenta uma **descoberta** ou um
> **ponto a verificar** acima.
