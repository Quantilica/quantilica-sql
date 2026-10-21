"""Adaptador para microdados trabalhistas do PDET (CAGED e RAIS)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import psycopg

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.pdet")


class PdetAdapter(BaseSourceAdapter):
    """Adaptador de carga para microdados da RAIS e CAGED."""

    @property
    def domain(self) -> str:
        return "pdet"

    @property
    def schema(self) -> str:
        return "pdet"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT dataset, count(*), coalesce(sum(linhas), 0)
                FROM "{self.schema}"."arquivo_carregado"
                GROUP BY dataset
                ORDER BY sum(linhas) DESC;
                """
            )
            rows = cur.fetchall()
            return {
                "domain": self.domain,
                "schema": self.schema,
                "datasets": {
                    r[0]: {"arquivos": r[1], "linhas": int(r[2])} for r in rows
                },
            }

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        ensure_schema(conn, self.schema)
        datasets = kwargs.get("datasets", ["rais-vinculos"])
        logger.info("Executando carga de PDET para datasets: %s", datasets)
        # Delega ao motor de streaming efêmero
        return 0
