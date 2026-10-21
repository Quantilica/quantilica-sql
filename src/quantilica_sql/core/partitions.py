"""Gerenciamento do ciclo de vida de partições declarativas no PostgreSQL."""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger("quantilica_sql.partitions")


def ensure_yearly_partition(
    conn: psycopg.Connection,
    schema: str,
    parent_table: str,
    year: int,
    tablespace: str = "pg_default",
) -> str:
    """Garante que uma partição anual existe e está vinculada à tabela pai."""
    part_name = f"{parent_table}_{year}"
    tb_clause = f'TABLESPACE "{tablespace}"' if tablespace != "pg_default" else ""

    check_sql = """
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s AND c.relname = %s;
    """
    with conn.cursor() as cur:
        cur.execute(check_sql, (schema, part_name))
        if cur.fetchone():
            return f'"{schema}"."{part_name}"'

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{part_name}"
        PARTITION OF "{schema}"."{parent_table}"
        FOR VALUES FROM ({year}) TO ({year + 1})
        {tb_clause};
        """
        cur.execute(create_sql)
        logger.info("Partição anual criada: %s.%s (%s)", schema, part_name, tablespace)

    if not conn.autocommit:
        conn.commit()
    return f'"{schema}"."{part_name}"'


def ensure_monthly_partition(
    conn: psycopg.Connection,
    schema: str,
    parent_table: str,
    year: int,
    month: int,
    tablespace: str = "pg_default",
) -> str:
    """Garante partição mensal nomeada {parent_table}_{year}_{month:02d}."""
    part_name = f"{parent_table}_{year}_{month:02d}"
    tb_clause = f'TABLESPACE "{tablespace}"' if tablespace != "pg_default" else ""

    check_sql = """
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s AND c.relname = %s;
    """
    with conn.cursor() as cur:
        cur.execute(check_sql, (schema, part_name))
        if cur.fetchone():
            return f'"{schema}"."{part_name}"'

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS "{schema}"."{part_name}"
        (LIKE "{schema}"."{parent_table}" INCLUDING ALL)
        {tb_clause};
        """
        cur.execute(create_sql)
        logger.info("Partição mensal criada: %s.%s (%s)", schema, part_name, tablespace)

    if not conn.autocommit:
        conn.commit()
    return f'"{schema}"."{part_name}"'


def list_partitions(
    conn: psycopg.Connection, schema: str, parent_table: str
) -> list[dict[str, Any]]:
    """Lista todas as partições filhas vinculadas a uma tabela pai."""
    query = """
    SELECT
        c.relname AS child_table,
        pg_get_expr(c.relpartbound, c.oid) AS partition_expression,
        pg_size_pretty(pg_total_relation_size(c.oid)) AS size_pretty,
        spc.spcname AS tablespace
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_tablespace spc ON spc.oid = c.reltablespace
    WHERE i.inhparent = %s::regclass
    ORDER BY c.relname;
    """
    full_parent = f'"{schema}"."{parent_table}"'
    with conn.cursor() as cur:
        cur.execute(query, (full_parent,))
        rows = cur.fetchall()
        return [
            {
                "child_table": r[0],
                "expression": r[1],
                "size_pretty": r[2],
                "tablespace": r[3] or "pg_default",
            }
            for r in rows
        ]
