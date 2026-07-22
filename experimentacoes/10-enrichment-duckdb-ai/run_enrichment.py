"""10 · Enrichment com duckdb-ai — escrevendo a clean numa DuckLake local.

Mesma prova do enrichment_duckdb_ai.sql (RAW -> enrichment (LLM) -> CLEAN via
ai_extract_record), mas em Python e **gravando na DuckLake** — pra testar a paridade
de storage ("o duckdb-ai escreve bem no lake?") e imprimir o custo por run.

Provider = Anthropic (mesmo do ai_enrichment atual). Config por ENV — NUNCA hardcode a chave:
  export ANTHROPIC_API_KEY="sk-ant-..."

Rodar:
  pip install "duckdb>=1.1"
  python run_enrichment.py

⚠️ Faz chamadas reais de LLM (custa tokens). Não rode em CI.
"""
from __future__ import annotations

import os
import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
CATALOG = os.path.join(HERE, ".out", "enrichment.ducklake")   # catálogo DuckLake local
DATA_PATH = os.path.join(HERE, ".out", "data")                # parquet da DuckLake
os.makedirs(DATA_PATH, exist_ok=True)

if not os.environ.get("ANTHROPIC_API_KEY"):
    raise SystemExit("defina ANTHROPIC_API_KEY no ambiente antes de rodar (ver README).")

con = duckdb.connect()
con.execute("INSTALL ai FROM community; LOAD ai;")
con.execute("INSTALL ducklake; LOAD ducklake;")

# provider Anthropic; a chave vem do ANTHROPIC_API_KEY (não passamos API_KEY aqui).
con.execute("""
    CREATE OR REPLACE SECRET strattum_ai (
        TYPE duckdb_ai, AI_PROVIDER 'anthropic', MODEL 'claude-haiku-4-5'
    )
""")
con.execute("SET duckdb_ai_max_concurrent_requests = 8")
con.execute("SET duckdb_ai_cache = true")
con.execute("SET duckdb_ai_prompt_cache = true")

# DuckLake local (catálogo em arquivo + parquet em disco) — sem precisar de Postgres/MinIO.
# Em produção troca por `ducklake:postgres:...` + DATA_PATH s3://… (ver tarefa 01).
con.execute(f"ATTACH 'ducklake:{CATALOG}' AS lake (DATA_PATH '{DATA_PATH}')")
con.execute("CREATE SCHEMA IF NOT EXISTS lake.raw")
con.execute("CREATE SCHEMA IF NOT EXISTS lake.enrichment")
con.execute("CREATE SCHEMA IF NOT EXISTS lake.clean")

# 1) RAW na DuckLake
con.execute("""
    CREATE OR REPLACE TABLE lake.raw.calls AS
    SELECT * FROM (VALUES
      (1, 'Cliente ACME Metalúrgica pediu proposta de 50 licenças. Quer fechar até o fim do mês; citou o concorrente DataFoo.'),
      (2, 'Ligação rápida com a Nova Fintech: só curiosidade, sem orçamento aprovado, pediu pra falar de novo no Q4.'),
      (3, 'Diretor da Sul Logística quer POC urgente pra 3 plantas, orçamento de ~200 mil, decisão essa semana.'),
      (4, 'Contato do Banco Meridiano: interesse em compliance/LGPD, ainda mapeando fornecedores, prazo indefinido.')
    ) AS t(call_id, transcript)
""")

# 2) ENRICHMENT (LLM) -> STRUCT tipado, gravado na camada enrichment da DuckLake
SCHEMA = """{
  "type": "object",
  "properties": {
    "company":              {"type": "string"},
    "intent":               {"type": "string"},
    "deal_size_brl":        {"type": "integer"},
    "urgency":              {"type": "string", "enum": ["low","medium","high"]},
    "competitor_mentioned": {"type": "boolean"}
  },
  "required": ["company","intent","urgency"]
}"""
con.execute(f"""
    CREATE OR REPLACE TABLE lake.enrichment.calls AS
    SELECT call_id, ai_extract_record(transcript, '{SCHEMA}') AS profile
    FROM lake.raw.calls
""")

# 3) CLEAN — achata o struct (no dbt real, aqui entraria o JOIN com raw/federation)
con.execute("""
    CREATE OR REPLACE TABLE lake.clean.calls AS
    SELECT
        call_id,
        profile.company              AS company,
        profile.intent               AS intent,
        profile.deal_size_brl        AS deal_size_brl,
        profile.urgency              AS urgency,
        profile.competitor_mentioned AS competitor_mentioned
    FROM lake.enrichment.calls
""")

print("\n── CLEAN (na DuckLake lake.clean.calls) ──")
for row in con.execute("SELECT * FROM lake.clean.calls ORDER BY call_id").fetchall():
    print("  ", row)

# 4) custo / tokens / retry / cache por chamada — parity "budget de custo"
print("\n── ai_usage() (tokens, custo, retry, cache) ──")
usage = con.execute("SELECT * FROM ai_usage()").fetch_df()
print(usage.to_string(index=False))

print("\n── versão da extensão (maturidade) ──")
print(con.execute(
    "SELECT extension_name, extension_version FROM duckdb_extensions() WHERE extension_name='ai'"
).fetchall())

print(f"\n✅ OK — RAW → enrichment (LLM) → CLEAN gravado na DuckLake em {CATALOG}")
print("   (prova: o duckdb-ai escreve a camada enrichment no lake, numa engine só.)")
