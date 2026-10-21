"""Adaptadores para fontes reguladas e meteorológicas (CVM, INMET, ANP)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import psycopg

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.domain")


class CvmAdapter(BaseSourceAdapter):
    """Adaptador de carga para dados abertos da CVM."""

    @property
    def domain(self) -> str:
        return "cvm"

    @property
    def schema(self) -> str:
        return "cvm"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM information_schema.tables WHERE table_schema = '{self.schema}';"
            )
            return {
                "domain": self.domain,
                "schema": self.schema,
                "tabelas": cur.fetchone()[0],
            }

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        ensure_schema(conn, self.schema)
        return 0


class InmetAdapter(BaseSourceAdapter):
    """Adaptador de carga para estações e séries horárias do INMET BDMEP."""

    @property
    def domain(self) -> str:
        return "inmet"

    @property
    def schema(self) -> str:
        return "inmet"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM information_schema.tables WHERE table_schema = '{self.schema}';"
            )
            return {
                "domain": self.domain,
                "schema": self.schema,
                "tabelas_particionadas": cur.fetchone()[0],
            }

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        ensure_schema(conn, self.schema)
        return 0


class AnpAdapter(BaseSourceAdapter):
    """Adaptador de carga para preços e produção de combustíveis da ANP."""

    @property
    def domain(self) -> str:
        return "anp"

    @property
    def schema(self) -> str:
        return "anp"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM information_schema.tables WHERE table_schema = '{self.schema}';"
            )
            return {
                "domain": self.domain,
                "schema": self.schema,
                "tabelas": cur.fetchone()[0],
            }

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        ensure_schema(conn, self.schema)
        return 0
