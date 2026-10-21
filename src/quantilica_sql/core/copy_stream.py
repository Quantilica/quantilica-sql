"""Utilitários de alto desempenho para streaming via protocolo COPY do PostgreSQL."""

from __future__ import annotations

import io
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg

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
    cols_sql = f'({", ".join(f'"{c}"' for c in columns)})' if columns else ""
    copy_sql = f"""
        COPY "{schema}"."{table}" {cols_sql}
        FROM STDIN WITH (
            FORMAT csv,
            DELIMITER '{delimiter}',
            NULL '{null_value}',
            QUOTE '{quote}',
            ENCODING '{encoding}'
        );
    """
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            with open(file_path, "rb") as f:
                while chunk := f.read(1024 * 1024):  # blocos de 1 MB
                    copy.write(chunk)

    if not conn.autocommit:
        conn.commit()

    # Obtém contagem aproximada/atual
    with conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM "{schema}"."{table}";')
        return int(cur.fetchone()[0])


def copy_from_rows(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    columns: list[str],
    rows: Iterable[Iterable[Any]],
    delimiter: str = "\t",
    chunk_size: int = 10000,
) -> int:
    """Escreve um gerador ou iterável de tuplas diretamente via COPY bufferizado."""
    cols_sql = f'({", ".join(f'"{c}"' for c in columns)})'
    copy_sql = f"""
        COPY "{schema}"."{table}" {cols_sql}
        FROM STDIN WITH (
            FORMAT csv,
            DELIMITER '{delimiter}',
            NULL '',
            QUOTE '"',
            ENCODING 'UTF8'
        );
    """
    total = 0
    buf = io.StringIO()
    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            for row in rows:
                row_str = delimiter.join(
                    ""
                    if val is None
                    else str(val).replace("\t", " ").replace("\n", " ")
                    for val in row
                )
                buf.write(row_str + "\n")
                total += 1
                if total % chunk_size == 0:
                    copy.write(buf.getvalue().encode("utf-8"))
                    buf.seek(0)
                    buf.truncate(0)

            if buf.tell() > 0:
                copy.write(buf.getvalue().encode("utf-8"))

    if not conn.autocommit:
        conn.commit()

    return total
