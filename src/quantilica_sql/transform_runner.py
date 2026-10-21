"""Execução de transformações SQL para materialização na camada analytics."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import psycopg

from quantilica_sql.config import get_settings
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.transform_runner")


class TransformRunner:
    """Executa scripts de transformação SQL e materializa data marts."""

    def __init__(self, target_schema: str | None = None) -> None:
        self.target_schema = target_schema or get_settings().analytics_schema

    def execute_sql_file(
        self,
        conn: psycopg.Connection,
        sql_file: Path,
        params: dict[str, Any] | None = None,
    ) -> bool:
        """Executa um arquivo SQL de transformação."""
        if not sql_file.exists():
            raise FileNotFoundError(f"Arquivo SQL não encontrado: {sql_file}")

        ensure_schema(conn, self.target_schema)
        sql_content = sql_file.read_text(encoding="utf-8")

        # Substituição simples de placeholders {{ schema }} se existirem
        sql_content = sql_content.replace("{{ target_schema }}", self.target_schema)

        logger.info("Executando transformação SQL: %s", sql_file.name)
        with conn.cursor() as cur:
            cur.execute(f'SET search_path = "{self.target_schema}", public;')
            cur.execute(sql_content)

        if not conn.autocommit:
            conn.commit()

        logger.info("Transformação concluída com sucesso: %s", sql_file.name)
        return True
