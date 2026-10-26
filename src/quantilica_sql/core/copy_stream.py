"""Utilitários de alto desempenho para streaming via protocolo COPY do PostgreSQL.

Dentro de ``atomic_file_load`` (``core.atomic``) o COPY não commita e marca
``_arquivo``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.core.atomic import tagged_copy_target
from quantilica_sql.core.txscope import commit_if_standalone

logger = logging.getLogger("quantilica_sql.copy_stream")


def _cols_sql(columns: list[str] | None) -> sql.Composable:
    if not columns:
        return sql.SQL("")
    names = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    return sql.SQL("({})").format(names)


def _csv_copy_sql(
    schema: str,
    table: str,
    columns: list[str] | None,
    delimiter: str,
    null_value: str,
    quote: str,
    encoding: str,
    header: bool = False,
) -> sql.Composable:
    hdr_clause = sql.SQL(", HEADER true") if header else sql.SQL("")
    return sql.SQL(
        """
        COPY {} {}
        FROM STDIN WITH (
            FORMAT csv,
            DELIMITER {},
            NULL {},
            QUOTE {},
            ENCODING {}{}
        );
        """
    ).format(
        sql.Identifier(schema, table),
        _cols_sql(columns),
        sql.Literal(delimiter),
        sql.Literal(null_value),
        sql.Literal(quote),
        sql.Literal(encoding),
        hdr_clause,
    )


def _rowcount(cur: psycopg.Cursor) -> int:
    return cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0


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
    header: bool = False,
) -> int:
    """Carrega dados de um arquivo em disco via streaming nativo COPY FROM STDIN.

    Com ``header=True`` a primeira linha do arquivo é o cabeçalho e não é carregada.
    """
    with tagged_copy_target(conn, schema, table, columns) as (sch, tbl, cols, result):
        copy_sql = _csv_copy_sql(
            sch, tbl, cols, delimiter, null_value, quote, encoding, header
        )
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                with open(file_path, "rb") as f:
                    while chunk := f.read(1024 * 1024):  # blocos de 1 MB
                        copy.write(chunk)
            rows_copied = _rowcount(cur)
    if result is not None:
        rows_copied = result["rows"]

    commit_if_standalone(conn)
    return rows_copied


def copy_from_rows(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    columns: list[str],
    rows: Iterable[Iterable[Any]],
) -> int:
    """Escreve um iterável de tuplas diretamente via COPY write_row nativo."""
    with tagged_copy_target(conn, schema, table, columns) as (sch, tbl, cols, result):
        copy_sql = sql.SQL("COPY {} {} FROM STDIN").format(
            sql.Identifier(sch, tbl), _cols_sql(cols)
        )
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for row in rows:
                    copy.write_row(row)
            rows_copied = _rowcount(cur)
    if result is not None:
        rows_copied = result["rows"]

    commit_if_standalone(conn)
    return rows_copied


def copy_from_stream(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    stream: Any,
    columns: list[str] | None = None,
    delimiter: str = ";",
    null_value: str = "",
    quote: str = '"',
    encoding: str = "UTF8",
    header: bool = False,
) -> int:
    """Carrega dados diretamente de um buffer/stream via streaming COPY FROM STDIN."""
    with tagged_copy_target(conn, schema, table, columns) as (sch, tbl, cols, result):
        copy_sql = _csv_copy_sql(
            sch, tbl, cols, delimiter, null_value, quote, encoding, header
        )
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                while chunk := stream.read(1024 * 1024):
                    if isinstance(chunk, str):
                        chunk = chunk.encode(encoding)
                    copy.write(chunk)
            rows_copied = _rowcount(cur)
    if result is not None:
        rows_copied = result["rows"]

    commit_if_standalone(conn)
    return rows_copied
