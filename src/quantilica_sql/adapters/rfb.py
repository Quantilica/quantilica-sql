"""Adaptador para dados abertos cadastrais de CNPJs da RFB."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import psycopg

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.rfb")


class RfbAdapter(BaseSourceAdapter):
    """Adaptador de carga para dados de CNPJs da Receita Federal."""

    @property
    def domain(self) -> str:
        return "rfb"

    @property
    def schema(self) -> str:
        return "rfb_cnpj"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s;
                """,
                (self.schema,),
            )
            tables_count = cur.fetchone()[0]
            return {
                "domain": self.domain,
                "schema": self.schema,
                "tabelas_particionadas": tables_count,
            }

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        ensure_schema(conn, self.schema)
        competencia = kwargs.get("competencia")
        logger.info("Executando carga de RFB CNPJ para competência: %s", competencia)
        raise NotImplementedError(
            f"O adaptador de carga '{self.domain}' ainda não foi portado para "
            f"o motor quantilica-sql v0.1.0. Utilize o loader canônico "
            f"'quantilica-workers/scripts/load_rfb_to_postgres.py' "
            f"ou consulte o plano de migração."
        )
