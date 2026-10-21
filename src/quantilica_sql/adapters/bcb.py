"""Adaptador para séries temporais do BCB SGS."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import psycopg

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.bcb")


class BcbAdapter(BaseSourceAdapter):
    """Adaptador de carga para séries temporais do BCB SGS."""

    @property
    def domain(self) -> str:
        return "bcb"

    @property
    def schema(self) -> str:
        return "bcb_sgs"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT count(*), coalesce(sum(linhas), 0) 
                FROM "{self.schema}"."arquivo_carregado" 
                WHERE dataset = 'series';
                """
            )
            series_count, total_rows = cur.fetchone()
            return {
                "domain": self.domain,
                "schema": self.schema,
                "series_carregadas": series_count,
                "linhas_totais": total_rows,
            }

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        ensure_schema(conn, self.schema)
        series_id = kwargs.get("series_id")
        logger.info("Carregando série BCB %s via adaptador", series_id)
        return 0
