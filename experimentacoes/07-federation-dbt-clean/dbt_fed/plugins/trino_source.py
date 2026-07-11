"""Plugin dbt-duckdb: fontes FEDERADAS via Trino.

Uma ÚNICA instância serve QUALQUER tabela/consulta de QUALQUER catálogo do Trino
(postgresql, delta, snowflake, iceberg...). Adicionar uma fonte federada nova =
umas linhas em `sources.yml` — nenhum Python novo. O JOIN vira SQL puro.

- Conexão (host/porta/user): vem do `profiles.yml`, bloco `plugins[].config`.
- Definição de cada tabela: vem do `meta:` da source. Três modos, do mais
  flexível ao mais simples:
    1. meta.query     — SQL Trino completo (permite filtrar/agregar/juntar já no Trino → pushdown)
    2. meta.relation  — 'catalog.schema.tabela'
    3. meta.catalog   — usa o catálogo + o schema/nome da própria source

O schema Arrow é INFERIDO dos dados — o plugin é agnóstico a colunas/tipos.
"""
from __future__ import annotations

import os
from typing import Any, Dict

import pyarrow as pa
import trino

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]) -> None:
        self.host = str(config.get("host", os.getenv("TRINO_HOST", "localhost")))
        self.port = int(config.get("port", os.getenv("TRINO_PORT", "8085")))
        self.user = str(config.get("user", "dbt"))
        self.http_scheme = str(config.get("http_scheme", "http"))
        self.default_catalog = config.get("catalog")

    def load(self, source_config: SourceConfig):
        catalog = source_config.get("catalog", self.default_catalog)

        query = source_config.get("query")
        if not query:
            relation = source_config.get("relation")
            if not relation:
                if not catalog:
                    raise ValueError(
                        "trino source: informe meta.query, meta.relation, ou "
                        "meta.catalog (+schema/nome da tabela)."
                    )
                relation = f"{catalog}.{source_config.schema}.{source_config.identifier}"
            query = f"SELECT * FROM {relation}"

        conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            http_scheme=self.http_scheme,
            catalog=catalog,  # pode ser None se a query for totalmente qualificada
        )
        cur = conn.cursor()
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        # Arrow com schema INFERIDO — nada de colunas/tipos hardcoded.
        return pa.table({c: [r[i] for r in rows] for i, c in enumerate(cols)})
