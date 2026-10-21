"""Rastreamento de idempotência via tabela arquivo_carregado por schema."""

from __future__ import annotations

import json
import logging
from typing import Any

import psycopg

logger = logging.getLogger("quantilica_sql.tracker")


def ensure_tracker_table(conn: psycopg.Connection, schema: str) -> None:
    """Garante que a tabela de idempotência existe no schema do domínio."""
    sql = f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."arquivo_carregado" (
        nome_arquivo TEXT NOT NULL PRIMARY KEY,
        dataset TEXT NOT NULL,
        linhas BIGINT NOT NULL DEFAULT 0,
        carregado_em TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        metadados JSONB
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    if not conn.autocommit:
        conn.commit()


def is_file_loaded(conn: psycopg.Connection, schema: str, filename: str) -> bool:
    """Verifica se um arquivo já foi carregado e registrado."""
    ensure_tracker_table(conn, schema)
    sql = f'SELECT 1 FROM "{schema}"."arquivo_carregado" WHERE nome_arquivo = %s;'
    with conn.cursor() as cur:
        cur.execute(sql, (filename,))
        return bool(cur.fetchone())


def record_loaded_file(
    conn: psycopg.Connection,
    schema: str,
    filename: str,
    dataset: str,
    rows_loaded: int,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Registra a conclusão de uma carga com sucesso."""
    ensure_tracker_table(conn, schema)
    sql = f"""
    INSERT INTO "{schema}"."arquivo_carregado" (nome_arquivo, dataset, linhas, metadados)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (nome_arquivo) DO UPDATE 
    SET linhas = EXCLUDED.linhas, 
        carregado_em = clock_timestamp(),
        metadados = EXCLUDED.metadados;
    """
    meta_json = json.dumps(metadata or {})
    with conn.cursor() as cur:
        cur.execute(sql, (filename, dataset, rows_loaded, meta_json))
    if not conn.autocommit:
        conn.commit()
