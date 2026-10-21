"""Utilitários para gestão e inspeção de tablespaces no PostgreSQL."""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger("quantilica_sql.tablespaces")


def list_tablespaces(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Lista todas as tablespaces disponíveis na instância PostgreSQL."""
    query = """
    SELECT 
        spcname, 
        coalesce(pg_tablespace_location(oid), '[default]') as location, 
        pg_size_pretty(pg_tablespace_size(oid)) as size_pretty,
        pg_tablespace_size(oid) as size_bytes
    FROM pg_tablespace
    ORDER BY pg_tablespace_size(oid) DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return [
            {
                "name": r[0],
                "location": r[1],
                "size_pretty": r[2],
                "size_bytes": r[3],
            }
            for r in rows
        ]


def move_table_to_tablespace(
    conn: psycopg.Connection, schema: str, table: str, tablespace: str
) -> bool:
    """Move uma tabela e seus índices para a tablespace indicada."""
    full_table = f'"{schema}"."{table}"'
    with conn.cursor() as cur:
        logger.info("Movendo %s para tablespace %s...", full_table, tablespace)
        cur.execute(f'ALTER TABLE {full_table} SET TABLESPACE "{tablespace}";')

        # Move índices associados
        cur.execute(
            """
            SELECT c.relname 
            FROM pg_class c 
            JOIN pg_index i ON i.indexrelid = c.oid 
            WHERE i.indrelid = %s::regclass;
            """,
            (full_table,),
        )
        indexes = cur.fetchall()
        for (idx,) in indexes:
            cur.execute(f'ALTER INDEX "{schema}"."{idx}" SET TABLESPACE "{tablespace}";')

    if not conn.autocommit:
        conn.commit()
    return True
