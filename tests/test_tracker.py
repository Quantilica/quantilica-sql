"""Testes unitários para o módulo de idempotência (tracker) do quantilica-sql."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from quantilica_sql.core.tracker import (
    ensure_tracker_table,
    is_file_loaded,
    record_loaded_file,
)


def _setup_mock_conn(autocommit: bool = False) -> tuple[MagicMock, MagicMock]:
    """Cria uma conexão e cursor mockados para testes."""
    mock_conn = MagicMock()
    mock_conn.autocommit = autocommit
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    return mock_conn, mock_cur


def test_ensure_tracker_table_ddl_and_index():
    """Valida DDL com PK (dataset, particao, nome_arquivo) e índice sha256."""
    mock_conn, mock_cur = _setup_mock_conn(autocommit=False)

    ensure_tracker_table(mock_conn, "pdet")

    sqls = [call[0][0].as_string() for call in mock_cur.execute.call_args_list]

    # Verifica se o schema foi criado
    schema_calls = [s for s in sqls if 'CREATE SCHEMA IF NOT EXISTS "pdet"' in s]
    assert len(schema_calls) == 1

    # Verifica DDL da tabela arquivo_carregado
    table_ddl_calls = [
        s for s in sqls if 'CREATE TABLE IF NOT EXISTS "pdet"."arquivo_carregado"' in s
    ]
    assert len(table_ddl_calls) == 1
    table_ddl = table_ddl_calls[0]
    assert "PRIMARY KEY (dataset, particao, nome_arquivo)" in table_ddl
    assert "nome_arquivo  TEXT        NOT NULL" in table_ddl
    assert "dataset       TEXT        NOT NULL" in table_ddl
    assert "particao      TEXT        NOT NULL DEFAULT ''" in table_ddl
    assert "sha256        TEXT" in table_ddl
    assert "linhas        BIGINT      NOT NULL DEFAULT 0" in table_ddl
    assert "carregado_em  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()" in table_ddl
    assert "metadados     JSONB       NOT NULL DEFAULT '{}'::jsonb" in table_ddl

    # Verifica criação do índice seguro de sha256
    expected_idx = (
        'CREATE INDEX IF NOT EXISTS "idx_pdet_arquivo_sha256" '
        'ON "pdet"."arquivo_carregado" (sha256)'
    )
    index_calls = [s for s in sqls if expected_idx in s]
    assert len(index_calls) == 1

    # Verifica commit
    assert mock_conn.commit.called


def test_ensure_tracker_table_autocommit():
    """Valida que quando autocommit=True, não chama conn.commit()."""
    mock_conn, _ = _setup_mock_conn(autocommit=True)

    ensure_tracker_table(mock_conn, "rfb")

    mock_conn.commit.assert_not_called()


def test_is_file_loaded_without_sha256():
    """Valida verificação de arquivo carregado sem conferência de sha256."""
    mock_conn, mock_cur = _setup_mock_conn()

    # Cenário 1: Arquivo carregado (registro encontrado)
    mock_cur.fetchone.return_value = ("hash_qualquer",)
    loaded = is_file_loaded(mock_conn, "pdet", "estabelecimentos_2024.csv")
    assert loaded is True

    # Verifica a query SQL gerada sem dataset
    last_call = mock_cur.execute.call_args_list[-1]
    query_str = last_call[0][0].as_string()
    params = last_call[0][1]
    expected_query = (
        'SELECT sha256 FROM "pdet"."arquivo_carregado" WHERE nome_arquivo = %s;'
    )
    assert expected_query in query_str
    assert params == ("estabelecimentos_2024.csv",)

    # Cenário 2: Arquivo não carregado (registro não encontrado)
    mock_cur.fetchone.return_value = None
    loaded = is_file_loaded(mock_conn, "pdet", "nao_existe.csv")
    assert loaded is False


def test_is_file_loaded_with_dataset_and_particao():
    """Valida filtro por dataset e particao em is_file_loaded."""
    mock_conn, mock_cur = _setup_mock_conn()
    mock_cur.fetchone.return_value = ("hash123",)

    loaded = is_file_loaded(
        mock_conn,
        "pdet",
        "vinculos_2024.csv",
        dataset="rais_vinculos",
        particao="2024",
    )
    assert loaded is True

    last_call = mock_cur.execute.call_args_list[-1]
    query_str = last_call[0][0].as_string()
    params = last_call[0][1]
    assert (
        'SELECT sha256 FROM "pdet"."arquivo_carregado" '
        "WHERE nome_arquivo = %s AND dataset = %s AND particao = %s;"
    ) in query_str
    assert params == ("vinculos_2024.csv", "rais_vinculos", "2024")


def test_is_file_loaded_with_sha256():
    """Valida conferência de integridade via sha256."""
    mock_conn, mock_cur = _setup_mock_conn()

    # Cenário 1: SHA256 coincide -> True
    mock_cur.fetchone.return_value = ("abc123hash",)
    assert (
        is_file_loaded(
            mock_conn,
            "cvm",
            "fii_2024.csv",
            dataset="fii",
            particao="2024",
            sha256="abc123hash",
        )
        is True
    )

    # Cenário 2: SHA256 difere do gravado -> False (precisa recarregar)
    mock_cur.fetchone.return_value = ("hash_antigo",)
    assert (
        is_file_loaded(
            mock_conn,
            "cvm",
            "fii_2024.csv",
            dataset="fii",
            particao="2024",
            sha256="hash_novo",
        )
        is False
    )

    # Cenário 3: Registro existe mas sha256 no banco é None -> False
    mock_cur.fetchone.return_value = (None,)
    assert (
        is_file_loaded(
            mock_conn,
            "cvm",
            "fii_2024.csv",
            dataset="fii",
            particao="2024",
            sha256="hash_novo",
        )
        is False
    )

    # Cenário 4: Registro não encontrado no banco -> False
    mock_cur.fetchone.return_value = None
    assert (
        is_file_loaded(
            mock_conn,
            "cvm",
            "fii_inexistente.csv",
            dataset="fii",
            sha256="qualquer_hash",
        )
        is False
    )


def test_record_loaded_file_with_defaults():
    """Valida inserção de registro de arquivo carregado com valores default."""
    mock_conn, mock_cur = _setup_mock_conn(autocommit=False)

    record_loaded_file(
        mock_conn,
        schema="sidra",
        filename="ipca_1737.json",
        dataset="ipca",
        rows_loaded=500,
    )

    last_call = mock_cur.execute.call_args_list[-1]
    query_str = last_call[0][0].as_string()
    params = last_call[0][1]

    # Verifica estrutura do INSERT com ON CONFLICT
    assert 'INSERT INTO "sidra"."arquivo_carregado"' in query_str
    assert "ON CONFLICT (dataset, particao, nome_arquivo) DO UPDATE" in query_str
    assert "SET linhas = EXCLUDED.linhas," in query_str
    assert (
        'sha256 = COALESCE(EXCLUDED.sha256, "arquivo_carregado".sha256),' in query_str
    )
    assert "carregado_em = clock_timestamp()," in query_str
    assert "metadados = EXCLUDED.metadados;" in query_str

    # Verifica parâmetros com defaults: particao="", sha256=None, metadados="{}"
    assert params == ("ipca_1737.json", "ipca", "", None, 500, "{}")
    assert mock_conn.commit.called


def test_record_loaded_file_with_particao_and_sha256():
    """Valida inserção com particao, sha256 e metadados customizados."""
    mock_conn, mock_cur = _setup_mock_conn(autocommit=True)

    metadata = {"origem": "ftp", "tentativa": 1, "tamanho_bytes": 1048576}
    record_loaded_file(
        mock_conn,
        schema="rfb",
        filename="estabelecimentos.zip",
        dataset="cnpj_estabelecimentos",
        rows_loaded=1250000,
        particao="2026-10",
        sha256="deadbeef12345678",
        metadata=metadata,
    )

    last_call = mock_cur.execute.call_args_list[-1]
    params = last_call[0][1]

    assert params == (
        "estabelecimentos.zip",
        "cnpj_estabelecimentos",
        "2026-10",
        "deadbeef12345678",
        1250000,
        json.dumps(metadata),
    )
    # Com autocommit=True, commit não deve ser chamado
    mock_conn.commit.assert_not_called()
