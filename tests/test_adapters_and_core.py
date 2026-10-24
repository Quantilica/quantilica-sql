"""Testes unitários para adaptadores, copy_stream e partições do quantilica-sql."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from quantilica_sql.adapters import get_adapter
from quantilica_sql.core.partitions import (
    ensure_monthly_partition,
    ensure_yearly_partition,
)


def test_adapters_raise_not_implemented():
    """Valida que todos os adaptadores stub levantam NotImplementedError no load."""
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = MagicMock()

    for domain in ["pdet", "rfb-cnpj", "sidra", "bcb", "cvm", "inmet", "anp"]:
        adapter = get_adapter(domain)
        with pytest.raises(NotImplementedError) as exc_info:
            adapter.load(mock_conn)
        assert adapter.domain in str(exc_info.value) or "não foi" in str(exc_info.value)


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
