# 10 · Enrichment com `duckdb-ai` — RAW → enrichment (LLM) → CLEAN numa engine só

> Testa a **sugestão da [descobertas §6](../../documents/arquitetura/2.0-lake-aberto/descobertas.md)**:
> rodar a camada **`enrichment` (LLM)** como **SQL/dbt** com a extensão
> [`duckdb-ai`](https://github.com/leonardovida/duckdb-ai) (`INSTALL ai FROM community`), em vez
> do estágio Python separado do [`ai_enrichment_pipeline`](../../../strattum-data/services/pipelines/src/flows/ai_enrichment_pipeline.py).
> Se funcionar, `RAW → enrichment` vira **um model dbt** (`SELECT ai_extract_record(...) FROM raw`)
> — **uma engine só** (DuckDB) pra ingestão, enrichment, transform e federation.
>
> **A pergunta do teste:** o `duckdb-ai` tem **paridade** com o que o pipeline atual precisa?
> (structured output estável, cache/idempotência, retry, custo/budget, escrita na DuckLake).
> Alimenta a decisão da [tarefa 01 §enrichment](../../documents/arquitetura/2.0-lake-aberto/tarefas/01-lakehouse/README.md).

⚠️ **Não rode em CI nem em janela de benchmark** — faz chamadas reais de LLM (custa tokens).
Versão testada da extensão: **v0.4.7** (projeto novo → maturidade é o ponto a verificar).

---

## O que você precisa

1. **DuckDB CLI** ≥ 1.1 (ou o `duckdb` do venv dos experimentos).
2. **Chave da Anthropic** (o mesmo provider do `ai_enrichment` atual):
   ```bash
   export ANTHROPIC_API_KEY="sk-ant-..."      # só na sessão do shell; nunca em arquivo
   ```
   > Local, sem custo: dá pra trocar `anthropic` por `ollama` (`ollama serve && ollama pull …`)
   > nas duas primeiras linhas do SQL — mesma prova de conceito, modelo local.

## Rodar

**Caminho A — só SQL (a prova do "enrichment = model dbt"):**
```bash
cd experimentacoes/10-enrichment-duckdb-ai
duckdb < enrichment_duckdb_ai.sql
```

**Caminho B — Python, escrevendo a clean na DuckLake (testa parity de storage + custo):**
```bash
cd experimentacoes
source .venv/bin/activate
pip install "duckdb>=1.1"
python 10-enrichment-duckdb-ai/run_enrichment.py
```

## O que os scripts fazem (e o que estamos validando)

1. **RAW** — uma tabelinha de *call transcripts* crus (simula o que a fonte entrega — é o
   input do `ai_enrichment` real: rubrica + transcript).
2. **ENRICHMENT** — `ai_extract_record(transcript, <json schema>)` transforma cada linha num
   **STRUCT tipado** (company, intent, deal_size, urgency, competitor_mentioned). É exatamente
   o "transcript → colunas estruturadas" do pipeline atual, mas **em SQL**.
3. **CLEAN** — achata o struct em colunas (no dbt real, este é o ponto onde a clean **cruza**
   com `raw`/`federation`).
4. **Observabilidade** — `ai_usage()` mostra **tokens, custo estimado, retries e cache hit** por
   chamada (é o que precisamos pra **budget de custo**).
5. **(Caminho B)** grava a `enrichment` e a `clean` numa **DuckLake local** — testa se o
   `duckdb-ai` **escreve bem no lake** (parity de storage).

Knobs de produção exercitados (paridade com o `ai_enrichment`): `max_concurrent_requests`
(concorrência), `cache`/`cache_ttl_seconds` (idempotência/re-run barato), `prompt_cache`
(prompt caching Anthropic), `ai_recommended_batch_size` (rate-limit).

## O que me mandar
Cola a **saída dos dois scripts** (a tabela `clean_*` e o `ai_usage()`), e a **versão** da
extensão (`SELECT * FROM duckdb_extensions() WHERE extension_name='ai'`). Com isso eu preencho o
`RESULTADOS.md` marcando cada item do **checklist de paridade** (✅/🛑) e a recomendação final:
adotar o `duckdb-ai` no enrichment, ou manter o estágio Python.

> Referência de funções: [`docs/functions.md`](https://github.com/leonardovida/duckdb-ai/blob/main/docs/functions.md)
> · cookbooks (batch enrichment, lakehouse output, custo): `docs/cookbooks/` no repo. Clone local: `~/Repos/duckdb-ai`.
