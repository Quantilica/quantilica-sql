"""Carga atômica por arquivo: COPY, substituição da partição e ledger numa só transação.

Dentro de ``atomic_file_load`` os utilitários de ``copy_stream`` e do ``tracker``
não commitam sozinhos, e os dados entram marcados com ``_arquivo`` (via tabela
temporária), o que permite substituir a carga anterior do mesmo arquivo.
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Iterator
from pathlib import Path

import psycopg
from psycopg import sql

from quantilica_sql.core.tracker import ensure_tracker_table
from quantilica_sql.core.txscope import ACTIVE, Scope

logger = logging.getLogger("quantilica_sql.atomic")

SOURCE_COLUMN = "_arquivo"
TABLES_KEY = "_tabelas"
_STAGING_TABLE = "_qsql_stg"


class LegacyLoadError(RuntimeError):
    """Linhas registradas no ledger sem ``_arquivo``: não dá para substituí-las."""


def file_sha256(path: Path) -> str:
    """SHA-256 do arquivo: usa o sidecar ``.manifest.json`` e, sem ele, calcula."""
    from quantilica.core.files import sha256_file
    from quantilica.core.manifests import read_manifest_sidecar

    manifest = read_manifest_sidecar(path)
    if manifest is not None and manifest.sha256:
        return manifest.sha256
    return sha256_file(path)


def has_source_column(conn: psycopg.Connection, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
            (schema, table, SOURCE_COLUMN),
        )
        return cur.fetchone() is not None


def _data_columns(conn: psycopg.Connection, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name <> %s "
            "ORDER BY ordinal_position",
            (schema, table, SOURCE_COLUMN),
        )
        return [r[0] for r in cur.fetchall()]


@contextlib.contextmanager
def tagged_copy_target(
    conn: psycopg.Connection,
    schema: str,
    table: str,
    columns: list[str] | None,
) -> Iterator[tuple[str, str, list[str] | None, dict[str, int] | None]]:
    """Destino do COPY: a própria tabela, ou uma temporária que marca ``_arquivo``.

    Fora de ``atomic_file_load`` (ou em outro schema) devolve o destino original
    e ``None`` no resultado. Dentro, o COPY vai para uma tabela temporária e, ao
    sair, as linhas são inseridas na tabela real com ``_arquivo = <arquivo>``;
    ``result["rows"]`` traz a contagem inserida.
    """
    scope = ACTIVE.get()
    if (
        scope is None
        or scope.schema != schema
        or (columns is not None and SOURCE_COLUMN in columns)
        or not has_source_column(conn, schema, table)
    ):
        yield schema, table, columns, None
        return
    scope.touched.add(table)

    cols = columns if columns is not None else _data_columns(conn, schema, table)
    col_sql = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE TEMP TABLE {stg} AS SELECT {cols} FROM {tgt} WITH NO DATA"
            ).format(
                stg=sql.Identifier(_STAGING_TABLE),
                cols=col_sql,
                tgt=sql.Identifier(schema, table),
            )
        )
    result: dict[str, int] = {"rows": 0}
    yield "pg_temp", _STAGING_TABLE, cols, result
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "INSERT INTO {tgt} ({cols}, {src}) SELECT {cols}, %s FROM {stg}"
            ).format(
                tgt=sql.Identifier(schema, table),
                cols=col_sql,
                src=sql.Identifier(SOURCE_COLUMN),
                stg=sql.Identifier("pg_temp", _STAGING_TABLE),
            ),
            (scope.filename,),
        )
        result["rows"] = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
        cur.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(_STAGING_TABLE)))


def _previous_load(
    conn: psycopg.Connection,
    schema: str,
    filename: str,
    dataset: str,
    particao: str,
) -> tuple[int, set[str]] | None:
    """Linhas e tabelas da carga anterior do arquivo, segundo o ledger."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT linhas, metadados FROM {} "
                "WHERE nome_arquivo = %s AND dataset = %s AND particao = %s"
            ).format(sql.Identifier(schema, "arquivo_carregado")),
            (filename, dataset, particao),
        )
        row = cur.fetchone()
    if row is None:
        return None
    meta = row[1] or {}
    tables = set(meta.get(TABLES_KEY, []))
    if isinstance(meta.get("tabela"), str):
        tables.add(meta["tabela"].split(".")[-1])
    return int(row[0]), tables


def _delete_previous(
    conn: psycopg.Connection, schema: str, tables: set[str], filename: str
) -> int:
    deleted = 0
    for table in sorted(tables):
        if not has_source_column(conn, schema, table):
            continue
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE {} = %s").format(
                    sql.Identifier(schema, table), sql.Identifier(SOURCE_COLUMN)
                ),
                (filename,),
            )
            deleted += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    return deleted


@contextlib.contextmanager
def atomic_file_load(
    conn: psycopg.Connection,
    *,
    schema: str,
    filename: str,
    dataset: str,
    particao: str = "",
    sha256: str | None = None,
    table: str | None = None,
    replace: bool = True,
) -> Iterator[None]:
    """Transação única para carregar um arquivo (que pode alimentar várias tabelas).

    Entra: apaga a carga anterior do arquivo (``replace``), nas tabelas que o ledger
    registrou (``metadados._tabelas``/``tabela``) e em ``table``, se informada. Sai
    sem erro: commit de dados e ledger juntos. Com erro: rollback de tudo, nada fica
    carregado nem registrado. ``sha256`` e as tabelas tocadas são gravados no ledger
    por ``record_loaded_file``.

    Raises:
        LegacyLoadError: o ledger diz que o arquivo tinha linhas, mas nenhuma está
            marcada com ``_arquivo`` (carga anterior a este mecanismo); substituir
            duplicaria dados.
    """
    if ACTIVE.get() is not None:
        raise RuntimeError("atomic_file_load não pode ser aninhado")
    ensure_tracker_table(conn, schema)
    token = ACTIVE.set(Scope(schema, filename, sha256))
    try:
        with conn.transaction():
            if replace:
                previous = _previous_load(conn, schema, filename, dataset, particao)
                if previous is not None:
                    rows, tables = previous
                    if table:
                        tables.add(table)
                    deleted = _delete_previous(conn, schema, tables, filename)
                    if rows and deleted == 0:
                        raise LegacyLoadError(
                            f"{schema}: o ledger registra {rows} linhas para "
                            f"{filename!r}, mas nenhuma tem {SOURCE_COLUMN}. Remova "
                            "a carga antiga manualmente antes de recarregar."
                        )
            yield
    finally:
        ACTIVE.reset(token)
    # fora de conn.transaction(): em modo sem autocommit a transação externa
    # (implícita) ainda está aberta e precisa do commit explícito.
    if not conn.autocommit:
        conn.commit()
