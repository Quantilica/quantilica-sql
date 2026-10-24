"""Testes unitários para adaptadores, copy_stream e partições do quantilica-sql."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from quantilica_sql.adapters import get_adapter
from quantilica_sql.core.partitions import (
    ensure_monthly_partition,
    ensure_yearly_partition,
)


class MockDbState:
    """Simulador de estado do PostgreSQL para testar adaptadores e idempotência."""

    def __init__(self) -> None:
        self.loaded_files: set[tuple[str, str, str]] = set()
        self.recorded: list[tuple] = []
        self.sqls: list[tuple[str, tuple | None]] = []

    def make_conn(self, rowcount: int = 10) -> MagicMock:
        mock_conn = MagicMock()
        mock_conn.autocommit = False
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.rowcount = rowcount

        mock_copy = MagicMock()
        mock_cur.copy.return_value.__enter__.return_value = mock_copy

        def execute_side_effect(query, params=None):
            q_str = query.as_string() if hasattr(query, "as_string") else str(query)
            self.sqls.append((q_str, params))
            if "SELECT sha256 FROM" in q_str:
                fn = params[0] if params else ""
                ds = params[1] if len(params) > 1 else ""
                part = params[2] if len(params) > 2 else ""
                if (fn, ds, part) in self.loaded_files or (
                    fn,
                    "",
                    "",
                ) in self.loaded_files:
                    mock_cur.fetchone.return_value = ("mock_sha",)
                else:
                    mock_cur.fetchone.return_value = None
            elif "INSERT INTO" in q_str and "arquivo_carregado" in q_str:
                if params:
                    fn, ds, part = params[0], params[1], params[2]
                    self.loaded_files.add((fn, ds, part))
                    self.recorded.append(params)
            elif "SELECT EXISTS" in q_str or "SELECT 1 FROM pg_class" in q_str:
                mock_cur.fetchone.return_value = (True,)
            elif "SELECT count(*)" in q_str:
                mock_cur.fetchone.return_value = (1, 100)
            elif "SELECT dataset, count(*)" in q_str:
                mock_cur.fetchall.return_value = [("caged", 1, 100)]
            elif "SELECT column_name FROM information_schema.columns" in q_str:
                mock_cur.fetchall.return_value = [("codigo",), ("descricao",)]

        mock_cur.execute.side_effect = execute_side_effect
        return mock_conn


def test_stub_adapters_raise_not_implemented():
    """Valida que adaptadores stub levantam NotImplementedError no load."""
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = MagicMock()

    for domain in ["cvm", "inmet", "anp"]:
        adapter = get_adapter(domain)
        with pytest.raises(NotImplementedError) as exc_info:
            adapter.load(mock_conn)
        assert adapter.domain in str(exc_info.value) or "não foi" in str(exc_info.value)


def test_sidra_adapter_load_and_idempotence():
    """Valida carga do SidraAdapter com sucesso, retorno de linhas e idempotência."""
    state = MockDbState()
    mock_conn = state.make_conn(rowcount=5)
    adapter = get_adapter("sidra")
    assert adapter.domain == "sidra"
    assert adapter.schema == "ibge_sidra"

    rows = [
        {
            "tabela_sidra_id": "1732",
            "localidade_id": 1,
            "dimensao_id": 2,
            "periodo_id": 2026,
            "modificacao": "2026-01-01",
            "ativo": True,
            "v": "100.5",
        }
    ]

    with tempfile.NamedTemporaryFile(suffix="_1732.json", mode="w", delete=False) as f:
        json.dump(rows, f)
        temp_path = Path(f.name)

    try:
        # 1ª Carga: deve processar e retornar o total de linhas
        loaded = adapter.load(mock_conn, source_path=temp_path, table_id="1732")
        assert loaded == 5
        assert len(state.recorded) == 1
        fn, ds, part, sha, linhas, meta = state.recorded[0]
        assert fn == temp_path.name
        assert ds == "sidra"
        assert part == "1732"
        assert linhas == 5

        # Status: deve retornar contagens
        status_info = adapter.status(mock_conn)
        assert status_info["domain"] == "sidra"
        assert status_info["schema"] == "ibge_sidra"
        assert status_info["tabelas_carregadas"] >= 0

        # 2ª Carga do mesmo arquivo: idempotência deve retornar 0 e não gravar duplicado
        loaded_second = adapter.load(mock_conn, source_path=temp_path, table_id="1732")
        assert loaded_second == 0
        assert len(state.recorded) == 1  # não houve novo registro
    finally:
        temp_path.unlink(missing_ok=True)


def test_bcb_adapter_load_and_idempotence():
    """Valida carga do BcbAdapter com sucesso, retorno de linhas e idempotência."""
    state = MockDbState()
    mock_conn = state.make_conn(rowcount=8)
    adapter = get_adapter("bcb")
    assert adapter.domain == "bcb"
    assert adapter.schema == "bcb_sgs"

    observations = [
        {"data": "01/01/2026", "valor": "12.50"},
        {"data": "01/02/2026", "valor": "13.10"},
    ]

    with tempfile.NamedTemporaryFile(suffix="_432.json", mode="w", delete=False) as f:
        json.dump(observations, f)
        temp_path = Path(f.name)

    try:
        # 1ª Carga
        loaded = adapter.load(mock_conn, source_path=temp_path, series_id=432)
        assert loaded == 8
        assert len(state.recorded) == 1
        fn, ds, part, sha, linhas, meta = state.recorded[0]
        assert fn == temp_path.name
        assert ds == "bcb"
        assert part == "432"
        assert linhas == 8

        # Status
        status_info = adapter.status(mock_conn)
        assert status_info["domain"] == "bcb"
        assert status_info["schema"] == "bcb_sgs"

        # 2ª Carga: idempotência
        loaded_second = adapter.load(mock_conn, source_path=temp_path, series_id=432)
        assert loaded_second == 0
        assert len(state.recorded) == 1
    finally:
        temp_path.unlink(missing_ok=True)


def test_pdet_adapter_load_and_idempotence():
    """Valida carga do PdetAdapter via DataFrame e idempotência."""
    state = MockDbState()
    mock_conn = state.make_conn(rowcount=12)
    adapter = get_adapter("pdet")
    assert adapter.domain == "pdet"
    assert adapter.schema == "pdet"

    df = pl.DataFrame({"codigo": ["A", "B"], "descricao": ["Desc A", "Desc B"]})

    # 1ª Carga
    loaded = adapter.load(
        mock_conn,
        df=df,
        dataset="caged",
        particao="2026",
        filename="caged_2026.csv",
    )
    assert loaded == 12
    assert len(state.recorded) == 1
    fn, ds, part, sha, linhas, meta = state.recorded[0]
    assert fn == "caged_2026.csv"
    assert ds == "caged"
    assert part == "2026"
    assert linhas == 12

    # Status
    status_info = adapter.status(mock_conn)
    assert status_info["domain"] == "pdet"
    assert "datasets" in status_info

    # 2ª Carga: idempotência
    loaded_second = adapter.load(
        mock_conn,
        df=df,
        dataset="caged",
        particao="2026",
        filename="caged_2026.csv",
    )
    assert loaded_second == 0
    assert len(state.recorded) == 1


def test_rfb_adapter_load_and_idempotence():
    """Valida carga do RfbAdapter via DataFrame e idempotência."""
    state = MockDbState()
    mock_conn = state.make_conn(rowcount=15)
    adapter = get_adapter("rfb")
    assert adapter.domain == "rfb"
    assert adapter.schema == "rfb_cnpj"

    df = pl.DataFrame({"codigo": ["101", "102"], "descricao": ["Ativ 1", "Ativ 2"]})

    # 1ª Carga
    loaded = adapter.load(
        mock_conn,
        df=df,
        table_name="cnaes",
        competencia="2026-10",
        filename="cnaes_2026-10.csv",
    )
    assert loaded == 15
    assert len(state.recorded) == 1
    fn, ds, part, sha, linhas, meta = state.recorded[0]
    assert fn == "cnaes_2026-10.csv"
    assert ds == "rfb_cnpj"
    assert part == "2026-10"
    assert linhas == 15

    # Status
    status_info = adapter.status(mock_conn)
    assert status_info["domain"] == "rfb"
    assert status_info["schema"] == "rfb_cnpj"

    # 2ª Carga: idempotência
    loaded_second = adapter.load(
        mock_conn,
        df=df,
        table_name="cnaes",
        competencia="2026-10",
        filename="cnaes_2026-10.csv",
    )
    assert loaded_second == 0
    assert len(state.recorded) == 1


def test_ensure_yearly_partition_sql():
    """Valida que ensure_yearly_partition emite PARTITION OF com range anual."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = None  # Partição não existe ainda

    part_name = ensure_yearly_partition(mock_conn, "pdet", "rais_vinculos", 2024)
    assert part_name == '"pdet"."rais_vinculos_2024"'

    create_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "CREATE TABLE IF NOT EXISTS" in str(call)
    ]
    assert len(create_calls) == 1
    call_sql = create_calls[0][0][0].as_string()
    assert 'PARTITION OF "pdet"."rais_vinculos"' in call_sql
    assert "FOR VALUES FROM (2024) TO (2025)" in call_sql


def test_ensure_monthly_partition_sql():
    """Valida que ensure_monthly_partition emite range mensal de datas."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchone.return_value = None  # Partição não existe ainda

    part_name = ensure_monthly_partition(
        mock_conn, "rfb_cnpj", "estabelecimentos", 2026, 10
    )
    assert part_name == '"rfb_cnpj"."estabelecimentos_2026_10"'

    create_calls = [
        call
        for call in mock_cur.execute.call_args_list
        if "CREATE TABLE IF NOT EXISTS" in str(call)
    ]
    assert len(create_calls) == 1
    call_sql = create_calls[0][0][0].as_string()
    assert 'PARTITION OF "rfb_cnpj"."estabelecimentos"' in call_sql
    assert "FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')" in call_sql
