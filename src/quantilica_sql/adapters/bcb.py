"""Adaptador para séries temporais do BCB SGS."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

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
            query = sql.SQL(
                """
                SELECT count(*), coalesce(sum(linhas), 0) 
                FROM {schema}.{table} 
                WHERE dataset = 'series';
                """
            ).format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier("arquivo_carregado"),
            )
            cur.execute(query)
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
        raise NotImplementedError(
            f"O adaptador de carga '{self.domain}' ainda não foi portado para "
            f"o motor quantilica-sql v0.1.0. Utilize o pacote 'bcb-sgs-sql' "
            f"ou consulte o plano de migração."
        )
