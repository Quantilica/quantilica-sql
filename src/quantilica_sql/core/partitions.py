"""Gerenciamento do ciclo de vida de partições declarativas no PostgreSQL."""

from __future__ import annotations

import logging
from typing import Any

import psycopg
from psycopg import sql

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

    check_sql = """
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s AND c.relname = %s;
    """
    with conn.cursor() as cur:
        cur.execute(check_sql, (schema, part_name))
        if cur.fetchone():
            return f'"{schema}"."{part_name}"'

        tb_clause = (
            sql.SQL("TABLESPACE {}").format(sql.Identifier(tablespace))
            if tablespace != "pg_default"
            else sql.SQL("")
        )
        target_part = sql.SQL("{}.{}").format(
            sql.Identifier(schema), sql.Identifier(part_name)
        )
        target_parent = sql.SQL("{}.{}").format(
            sql.Identifier(schema), sql.Identifier(parent_table)
        )

        create_sql = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}
            PARTITION OF {}
            FOR VALUES FROM ({}) TO ({})
            {};
            """
        ).format(
            target_part,
            target_parent,
            sql.Literal(year),
            sql.Literal(year + 1),
            tb_clause,
        )
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
    """Garante partição mensal vinculada à tabela pai declarativa."""
    part_name = f"{parent_table}_{year}_{month:02d}"

    check_sql = """
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s AND c.relname = %s;
    """
    with conn.cursor() as cur:
        cur.execute(check_sql, (schema, part_name))
        if cur.fetchone():
            return f'"{schema}"."{part_name}"'

        start_date = f"{year:04d}-{month:02d}-01"
        next_year = year + 1 if month == 12 else year
        next_month = 1 if month == 12 else month + 1
        end_date = f"{next_year:04d}-{next_month:02d}-01"

        tb_clause = (
            sql.SQL("TABLESPACE {}").format(sql.Identifier(tablespace))
            if tablespace != "pg_default"
            else sql.SQL("")
        )
        target_part = sql.SQL("{}.{}").format(
            sql.Identifier(schema), sql.Identifier(part_name)
        )
        target_parent = sql.SQL("{}.{}").format(
            sql.Identifier(schema), sql.Identifier(parent_table)
        )

        create_sql = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}
            PARTITION OF {}
            FOR VALUES FROM ({}) TO ({})
            {};
            """
        ).format(
            target_part,
            target_parent,
            sql.Literal(start_date),
            sql.Literal(end_date),
            tb_clause,
        )
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
    WHERE i.inhparent = (%s || '.' || %s)::regclass
    ORDER BY c.relname;
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema, parent_table))
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
