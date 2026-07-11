"""dbt-duckdb plugin — escreve o resultado do modelo DIRETO em Delta Lake.

Preenche o buraco do plugin nativo `delta.py` (que só tem `load`/leitura).
Aqui implementamos `store()`, o hook que a materialização `external` do
dbt-duckdb chama DEPOIS de computar o modelo. É o `dbt run` que invoca isto —
não há passo Python separado orquestrado fora do dbt.

Uso no modelo:

    {{ config(
        materialized='external',
        plugin='delta_writer',
        location='<temp>.parquet',          -- artefato efêmero (ver caveat)
        delta_table_path='/data/clean/orders',
        delta_mode='overwrite',             -- ou 'merge'
        delta_key='id'                      -- obrigatório se delta_mode='merge'
    ) }}

⚠️ CAVEAT (limite do dbt-duckdb, não deste plugin): a materialização `external`
SEMPRE escreve um parquet físico via `COPY TO` antes de chamar `store()`. Então
o fluxo real é: dbt computa → grava parquet temporário → `store()` relê e escreve
Delta. O parquet temporário é efêmero (removido no fim). Não existe, hoje, um
`COPY TO (FORMAT delta)` no DuckDB — logo "dbt materializa Delta em um único
write, sem intermediário" NÃO é alcançável. O ganho é: um único `dbt run`, sem
tabela `.duckdb` da clean e sem script Python pós-dbt no flow.
"""

from __future__ import annotations

import os
from typing import Any, Dict

import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import TargetConfig


class Plugin(BasePlugin):
    def initialize(self, plugin_config: Dict[str, Any]) -> None:
        # storage_options global (S3/MinIO) pode vir do profiles.yml
        self._storage_options = plugin_config.get("storage_options") or None

    def store(self, target_config: TargetConfig) -> None:
        cfg = target_config.config
        delta_path = cfg.get("delta_table_path")
        if not delta_path:
            raise ValueError("delta_table_path é obrigatório no config do modelo")

        mode = cfg.get("delta_mode", "overwrite")
        key = cfg.get("delta_key")

        # Relê o parquet que a materialização external acabou de escrever.
        parquet_path = target_config.location.path
        table = pq.read_table(parquet_path)

        so = self._storage_options

        if mode == "merge":
            if not key:
                raise ValueError("delta_key é obrigatório quando delta_mode='merge'")
            try:
                dt = DeltaTable(delta_path, storage_options=so) if so else DeltaTable(delta_path)
            except Exception:
                # Primeira execução — tabela ainda não existe: cria via overwrite.
                self._overwrite(delta_path, table, so)
                return
            (
                dt.merge(
                    source=table,
                    predicate=f"target.{key} = source.{key}",
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        else:
            self._overwrite(delta_path, table, so)

        # Limpa o parquet temporário — o artefato final é só o Delta.
        try:
            if os.path.isfile(parquet_path):
                os.remove(parquet_path)
        except OSError:
            pass

    @staticmethod
    def _overwrite(delta_path: str, table, storage_options) -> None:
        kwargs: Dict[str, Any] = {"mode": "overwrite", "schema_mode": "overwrite"}
        if storage_options:
            kwargs["storage_options"] = storage_options
        write_deltalake(delta_path, table, **kwargs)

    def default_materialization(self):
        return "external"
