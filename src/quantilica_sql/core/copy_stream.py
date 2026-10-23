"""Utilitários de alto desempenho para streaming via protocolo COPY do PostgreSQL."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

logger = logging.getLogger("quantilica_sql.copy_stream")


def copy_from_file(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    file_path: Path,
    columns: list[str] | None = None,
    delimiter: str = "\t",
    null_value: str = "",
    quote: str = '"',
    encoding: str = "UTF8",
) -> int:
    """Carrega dados de um arquivo em disco via streaming nativo COPY FROM STDIN."""
    target_table = sql.SQL("{}.{}").format(
        sql.Identifier(schema), sql.Identifier(table)
    )
    cols_sql = (
        sql.SQL("({})").format(sql.SQL(", ").join(sql.Identifier(c) for c in columns))
        if columns
        else sql.SQL("")
    )
    copy_sql = sql.SQL(
        """
        COPY {} {}
        FROM STDIN WITH (
            FORMAT csv,
            DELIMITER {},
            NULL {},
            QUOTE {},
            ENCODING {}
        );
        """
    ).format(
        target_table,
        cols_sql,
        sql.Literal(delimiter),
        sql.Literal(null_value),
        sql.Literal(quote),
        sql.Literal(encoding),
    )
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            with open(file_path, "rb") as f:
                while chunk := f.read(1024 * 1024):  # blocos de 1 MB
                    copy.write(chunk)
        rows_copied = (
            cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
        )

    if not conn.autocommit:
        conn.commit()

    return rows_copied


def copy_from_rows(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    columns: list[str],
    rows: Iterable[Iterable[Any]],
) -> int:
    """Escreve um iterável de tuplas diretamente via COPY write_row nativo."""
    target_table = sql.SQL("{}.{}").format(
        sql.Identifier(schema), sql.Identifier(table)
    )
    cols_sql = sql.SQL("({})").format(
        sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    )
    copy_sql = sql.SQL("COPY {} {} FROM STDIN").format(target_table, cols_sql)

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for row in rows:
                copy.write_row(row)
        rows_copied = (
            cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
        )

    if not conn.autocommit:
        conn.commit()

    return rows_copied
