"""Rastreamento de idempotência via tabela arquivo_carregado por schema."""

from __future__ import annotations

import json
import logging
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.core.txscope import ACTIVE, commit_if_standalone
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.tracker")


def ensure_tracker_table(conn: psycopg.Connection, schema: str) -> None:
    """Garante que a tabela de idempotência existe no schema do domínio."""
    ensure_schema(conn, schema)
    create_table_sql = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            nome_arquivo  TEXT        NOT NULL,
            dataset       TEXT        NOT NULL,
            particao      TEXT        NOT NULL DEFAULT '',
            sha256        TEXT,
            linhas        BIGINT      NOT NULL DEFAULT 0,
            carregado_em  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            metadados     JSONB       NOT NULL DEFAULT '{{}}'::jsonb,
            PRIMARY KEY (dataset, particao, nome_arquivo)
        );
        """
    ).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier("arquivo_carregado"),
    )
    idx_name = f"idx_{schema}_arquivo_sha256"
    create_index_sql = sql.SQL(
        "CREATE INDEX IF NOT EXISTS {idx_name} ON {schema}.{table} (sha256);"
    ).format(
        idx_name=sql.Identifier(idx_name),
        schema=sql.Identifier(schema),
        table=sql.Identifier("arquivo_carregado"),
    )
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
        cur.execute(create_index_sql)
    commit_if_standalone(conn)


def is_file_loaded(
    conn: psycopg.Connection,
    schema: str,
    filename: str,
    dataset: str | None = None,
    particao: str = "",
    sha256: str | None = None,
) -> bool:
    """Verifica se um arquivo já foi carregado e registrado."""
    ensure_tracker_table(conn, schema)
    where_clauses = [sql.SQL("nome_arquivo = %s")]
    params: list[Any] = [filename]

    if dataset is not None:
        where_clauses.append(sql.SQL("dataset = %s"))
        params.append(dataset)
        where_clauses.append(sql.SQL("particao = %s"))
        params.append(particao)

    where_sql = sql.SQL(" AND ").join(where_clauses)
    query = sql.SQL("SELECT sha256 FROM {schema}.{table} WHERE {where};").format(
        schema=sql.Identifier(schema),
        table=sql.Identifier("arquivo_carregado"),
        where=where_sql,
    )
    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        row = cur.fetchone()
        if not row:
            return False
        if sha256 is not None:
            return row[0] == sha256
        return True


def record_loaded_file(
    conn: psycopg.Connection,
    schema: str,
    filename: str,
    dataset: str,
    rows_loaded: int,
    particao: str = "",
    sha256: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Registra a conclusão de uma carga com sucesso."""
    scope = ACTIVE.get()
    if scope is not None:
        if sha256 is None:
            sha256 = scope.sha256
        if scope.touched:
            metadata = {**(metadata or {}), "_tabelas": sorted(scope.touched)}
    ensure_tracker_table(conn, schema)
    insert_sql = sql.SQL(
        """
        INSERT INTO {schema}.{table} (
            nome_arquivo, dataset, particao, sha256, linhas, metadados
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (dataset, particao, nome_arquivo) DO UPDATE
        SET linhas = EXCLUDED.linhas,
            sha256 = COALESCE(EXCLUDED.sha256, {table}.sha256),
            carregado_em = clock_timestamp(),
            metadados = EXCLUDED.metadados;
        """
    ).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier("arquivo_carregado"),
    )
    meta_json = json.dumps(metadata or {})
    with conn.cursor() as cur:
        cur.execute(
            insert_sql,
            (filename, dataset, particao, sha256, rows_loaded, meta_json),
        )
    commit_if_standalone(conn)


def file_status(
    conn: psycopg.Connection,
    schema: str,
    filename: str,
    dataset: str,
    particao: str,
    sha256: str,
) -> str:
    """Situação do arquivo no ledger frente ao hash atual.

    Returns:
        ``"new"`` (sem registro), ``"loaded"`` (mesmo hash), ``"changed"``
        (republicado com outro hash) ou ``"legacy"`` (registro sem hash, de antes
        do ledger por ``sha256``; não dá para saber se mudou).
    """
    ensure_tracker_table(conn, schema)
    query = sql.SQL(
        "SELECT sha256 FROM {} WHERE nome_arquivo = %s AND dataset = %s "
        "AND particao = %s"
    ).format(sql.Identifier(schema, "arquivo_carregado"))
    with conn.cursor() as cur:
        cur.execute(query, (filename, dataset, particao))
        row = cur.fetchone()
    if row is None:
        return "new"
    if row[0] is None:
        return "legacy"
    return "loaded" if row[0] == sha256 else "changed"


def backfill_sha256(
    conn: psycopg.Connection,
    schema: str,
    filename: str,
    dataset: str,
    particao: str,
    sha256: str,
) -> None:
    """Preenche o hash de um registro legado, sem recarregar nada."""
    query = sql.SQL(
        "UPDATE {} SET sha256 = %s WHERE nome_arquivo = %s AND dataset = %s "
        "AND particao = %s AND sha256 IS NULL"
    ).format(sql.Identifier(schema, "arquivo_carregado"))
    with conn.cursor() as cur:
        cur.execute(query, (sha256, filename, dataset, particao))
    commit_if_standalone(conn)
