"""Gerenciamento de conexões, pools e locks do PostgreSQL."""

from __future__ import annotations

import contextlib
import logging
import zlib
from collections.abc import Generator
from typing import Any

import psycopg
from sqlalchemy import Engine, create_engine

from quantilica_sql.config import get_settings

logger = logging.getLogger("quantilica_sql.database")

_ENGINE: Engine | None = None


def get_engine(db_url: str | None = None) -> Engine:
    """Retorna Engine SQLAlchemy singleton."""
    global _ENGINE
    url = db_url or get_settings().database_url
    if _ENGINE is None or str(_ENGINE.url) != url:
        sa_url = url
        if sa_url.startswith("postgresql://"):
            sa_url = sa_url.replace("postgresql://", "postgresql+psycopg://", 1)
        _ENGINE = create_engine(
            sa_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
    return _ENGINE


def get_connection(
    db_url: str | None = None, autocommit: bool = False
) -> psycopg.Connection:
    """Retorna uma conexão nativa psycopg3."""
    url = db_url or get_settings().normalize_db_url()
    return psycopg.connect(url, autocommit=autocommit)


def ensure_schema(conn: psycopg.Connection, schema_name: str) -> None:
    """Garante que o schema existe no banco."""
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
    if not conn.autocommit:
        conn.commit()


@contextlib.contextmanager
def pg_advisory_lock(
    conn: psycopg.Connection, lock_name: str
) -> Generator[bool, None, None]:
    """Context manager para lock consultivo transacional no PostgreSQL."""
    lock_id = zlib.crc32(lock_name.encode("utf-8")) & 0x7FFFFFFF
    acquired = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s);", (lock_id,))
            acquired = bool(cur.fetchone()[0])
        yield acquired
    finally:
        if acquired:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(%s);", (lock_id,))
            except Exception as exc:
                logger.warning("Erro ao liberar advisory lock %s: %s", lock_name, exc)


def inspect_schemas(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """Inspeciona todos os schemas de domínio, tamanhos e contagem de tabelas."""
    query = """
    SELECT 
        schemaname,
        count(*) as table_count,
        pg_size_pretty(sum(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::bigint) as total_size,
        sum(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::bigint as total_bytes
    FROM pg_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    GROUP BY schemaname
    ORDER BY sum(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        return [
            {
                "schema": r[0],
                "table_count": r[1],
                "size_pretty": r[2],
                "size_bytes": r[3] or 0,
            }
            for r in rows
        ]
