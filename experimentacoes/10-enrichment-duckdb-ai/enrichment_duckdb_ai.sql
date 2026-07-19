-- 10 · Enrichment com duckdb-ai — RAW → enrichment (LLM) → CLEAN numa engine só.
--
-- Prova que a camada `enrichment` da 2.0 pode ser um model dbt (SELECT ai_extract_record(...)
-- FROM raw) em vez de um estágio Python separado. Provider = Anthropic (o mesmo do
-- ai_enrichment atual). Rode com: `duckdb < enrichment_duckdb_ai.sql`
--
-- ⚠️ Faz chamadas reais de LLM (custa tokens). Precisa de ANTHROPIC_API_KEY no ambiente.

INSTALL ai FROM community;
LOAD ai;

-- Provider via secret (a chave NÃO vai na query — vem do ANTHROPIC_API_KEY do ambiente).
-- Em produção isto vem do secrets store (strattum_core.secrets), renderizado por run.
CREATE OR REPLACE SECRET strattum_ai (
    TYPE duckdb_ai,
    AI_PROVIDER 'anthropic',
    MODEL 'claude-haiku-4-5'
    -- API_KEY '...'   -- opcional; omitido → usa ANTHROPIC_API_KEY do ambiente
);

-- Knobs de produção (paridade com o ai_enrichment atual: concorrência, cache, prompt cache).
SET duckdb_ai_max_concurrent_requests = 8;   -- concorrência de chamadas
SET duckdb_ai_cache = true;                   -- cache de resposta (idempotência / re-run barato)
SET duckdb_ai_prompt_cache = true;            -- prompt caching ephemeral da Anthropic

------------------------------------------------------------------------------------------
-- 1) RAW — call transcripts crus (simula o input do ai_enrichment: rubrica + transcript).
------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE raw_calls AS
SELECT * FROM (VALUES
  (1, 'Cliente ACME Metalúrgica pediu proposta de 50 licenças. Quer fechar até o fim do mês; citou que está avaliando o concorrente DataFoo.'),
  (2, 'Ligação rápida com a Nova Fintech: só curiosidade, sem orçamento aprovado, pediu pra falar de novo no Q4.'),
  (3, 'Diretor da Sul Logística quer POC urgente pra 3 plantas, orçamento de ~200 mil, decisão essa semana. Sem concorrente citado.'),
  (4, 'Contato da Banco Meridiano: interesse em compliance/LGPD, ainda mapeando fornecedores, prazo indefinido.')
) AS t(call_id, transcript);

------------------------------------------------------------------------------------------
-- 2) ENRICHMENT — transcript -> STRUCT tipado. É o "transcript -> colunas estruturadas"
--    do pipeline atual, mas em SQL. O schema é constante (DuckDB precisa pra bindar o tipo).
------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE enrichment_calls AS
SELECT
    call_id,
    ai_extract_record(
        transcript,
        '{
          "type": "object",
          "properties": {
            "company":              {"type": "string"},
            "intent":               {"type": "string"},
            "deal_size_brl":        {"type": "integer"},
            "urgency":              {"type": "string", "enum": ["low", "medium", "high"]},
            "competitor_mentioned": {"type": "boolean"}
          },
          "required": ["company", "intent", "urgency"]
        }'
    ) AS profile
FROM raw_calls;

------------------------------------------------------------------------------------------
-- 3) CLEAN — achata o struct em colunas. No dbt real, é aqui que a clean cruza (JOIN) com
--    raw/federation. `materialized='table'` -> vira uma tabela da camada clean.
------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE clean_calls AS
SELECT
    call_id,
    profile.company              AS company,
    profile.intent               AS intent,
    profile.deal_size_brl        AS deal_size_brl,
    profile.urgency              AS urgency,
    profile.competitor_mentioned AS competitor_mentioned
FROM enrichment_calls;

SELECT '── CLEAN (enrichment achatado) ──' AS section;
SELECT * FROM clean_calls ORDER BY call_id;

------------------------------------------------------------------------------------------
-- 4) OBSERVABILIDADE — custo/tokens/retry/cache por chamada. É o que precisamos pra
--    "budget de custo" (parity com o ai_enrichment). Cola esta saída no RESULTADOS.md.
------------------------------------------------------------------------------------------
SELECT '── ai_usage() (tokens, custo, retry, cache) ──' AS section;
SELECT * FROM ai_usage();

-- Versão da extensão testada (anotar no RESULTADOS — maturidade v0.4.x).
SELECT '── versão da extensão ──' AS section;
SELECT extension_name, extension_version, installed
FROM duckdb_extensions() WHERE extension_name = 'ai';
