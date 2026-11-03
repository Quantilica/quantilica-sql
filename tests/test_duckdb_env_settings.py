"""QUANTILICA_DUCKDB_MEMORY/THREADS (de infra/lake-marts.sh) chegam à conexão."""

from __future__ import annotations

import pytest

pytest.importorskip("duckdb")

from quantilica_sql.lake.duck import (  # noqa: E402
    LakeError,
    connect_lake,
    duckdb_env_settings,
)


def _setting(conn, name: str) -> str:
    return str(conn.execute(f"SELECT current_setting('{name}')").fetchone()[0])


def test_sem_variaveis_mantem_defaults_do_duckdb(tmp_path, monkeypatch):
    import duckdb

    monkeypatch.delenv("QUANTILICA_DUCKDB_MEMORY", raising=False)
    monkeypatch.delenv("QUANTILICA_DUCKDB_THREADS", raising=False)
    padrao = duckdb.connect()
    conn = connect_lake(tmp_path / "lake")
    assert _setting(conn, "memory_limit") == _setting(padrao, "memory_limit")
    assert _setting(conn, "threads") == _setting(padrao, "threads")


def test_variaveis_vazias_equivalem_a_ausentes(tmp_path, monkeypatch):
    monkeypatch.setenv("QUANTILICA_DUCKDB_MEMORY", "  ")
    monkeypatch.setenv("QUANTILICA_DUCKDB_THREADS", "")
    assert duckdb_env_settings() == (None, None)
    connect_lake(tmp_path / "lake")  # não levanta


def test_memoria_e_threads_aplicadas_a_conexao(tmp_path, monkeypatch):
    monkeypatch.setenv("QUANTILICA_DUCKDB_MEMORY", "2GB")
    monkeypatch.setenv("QUANTILICA_DUCKDB_THREADS", "3")
    conn = connect_lake(tmp_path / "lake")
    # o DuckDB normaliza a unidade (ex.: '1.8 GiB'); basta ser bem menor que o padrão
    assert _setting(conn, "memory_limit").startswith(("1.8", "2.0", "1.9"))
    assert _setting(conn, "threads") == "3"


@pytest.mark.parametrize("valor", ["64GB", "512MiB", "1.5 GB", "80%", "2tb"])
def test_memoria_valida(valor):
    assert duckdb_env_settings({"QUANTILICA_DUCKDB_MEMORY": valor}) == (valor, None)


@pytest.mark.parametrize("valor", ["64", "muito", "GB", "-4GB", "64 giga", "1;DROP"])
def test_memoria_invalida_levanta_lakeerror(valor):
    with pytest.raises(LakeError, match="QUANTILICA_DUCKDB_MEMORY"):
        duckdb_env_settings({"QUANTILICA_DUCKDB_MEMORY": valor})


@pytest.mark.parametrize("valor", ["0", "-2", "abc", "2.5", "4 8"])
def test_threads_invalido_levanta_lakeerror(valor):
    with pytest.raises(LakeError, match="QUANTILICA_DUCKDB_THREADS"):
        duckdb_env_settings({"QUANTILICA_DUCKDB_THREADS": valor})


def test_connect_lake_propaga_erro_de_validacao(tmp_path, monkeypatch):
    monkeypatch.setenv("QUANTILICA_DUCKDB_THREADS", "zero")
    with pytest.raises(LakeError):
        connect_lake(tmp_path / "lake")
