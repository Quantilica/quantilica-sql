"""Adaptador para tabelas agregadas do IBGE SIDRA."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import psycopg

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.sidra")


class SidraAdapter(BaseSourceAdapter):
    """Adaptador de carga para dados do IBGE SIDRA."""

    @property
    def domain(self) -> str:
        return "sidra"

    @property
    def schema(self) -> str:
        return "ibge_sidra"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT count(*), coalesce(sum(linhas), 0) 
                FROM "{self.schema}"."arquivo_carregado" 
                WHERE dataset = 'tabela_sidra';
                """
            )
            tables_count, total_rows = cur.fetchone()
            return {
                "domain": self.domain,
                "schema": self.schema,
                "tabelas_carregadas": tables_count,
                "linhas_totais": total_rows,
            }

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        ensure_schema(conn, self.schema)
        table_id = kwargs.get("table_id")
        logger.info("Carregando tabela SIDRA %s via adaptador", table_id)
        # Integração com sidra-fetcher e copy_stream
        return 0
