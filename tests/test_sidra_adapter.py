"""SidraAdapter: carga de pasta do bruto (``--all``) e data da fonte.

Os testes de transformação usam um banco simulado (``MockDbState``) e capturam as
linhas entregues ao COPY. Os de integração exigem ``QUANTILICA_TEST_DATABASE_URL``
(banco descartável) e usam um schema próprio, removido ao final.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from pathlib import Path

import pytest
from test_adapters_and_core import MockDbState

from quantilica_sql.adapters import get_adapter
from quantilica_sql.adapters import sidra as sidra_mod
from quantilica_sql.adapters.sidra import SidraAdapter

HEADER = {
    "NC": "Nível Territorial (Código)",
    "NN": "Nível Territorial",
    "MC": "Unidade de Medida (Código)",
    "MN": "Unidade de Medida",
    "V": "Valor",
    "D1C": "Brasil (Código)",
    "D1N": "Brasil",
    "D2C": "Variável (Código)",
    "D2N": "Variável",
    "D3C": "Mês (Código)",
    "D3N": "Mês",
}


def _linha(valor: str, periodo: str, variavel: str = "63") -> dict[str, str]:
    return {
        "NC": "1",
        "NN": "Brasil",
        "MC": "2",
        "MN": "%",
        "V": valor,
        "D1C": "1",
        "D1N": "Brasil",
        "D2C": variavel,
        "D2N": "Variação mensal",
        "D3C": periodo,
        "D3N": "mês",
    }


def _grava(
    raiz: Path,
    nome: str,
    linhas: list[dict[str, str]],
    *,
    tabela: str = "1086",
    sidecar_modificacao: str | None = None,
) -> Path:
    """Grava a resposta crua do ``/values`` e, se pedido, o sidecar."""
    pasta = raiz / f"t-{tabela}"
    pasta.mkdir(parents=True, exist_ok=True)
    arq = pasta / nome
    arq.write_text(json.dumps([HEADER, *linhas]), encoding="utf-8")
    if sidecar_modificacao is not None:
        manifesto = {
            "source_id": "ibge-sidra",
            "dataset_id": f"t-{tabela}",
            "fetched_at": "2026-11-24T10:00:00+00:00",
            "metadata": {"modificacao": sidecar_modificacao, "periodos": ["202401"]},
        }
        arq.with_name(arq.name + ".manifest.json").write_text(
            json.dumps(manifesto), encoding="utf-8"
        )
    return arq


@pytest.fixture
def capturado(monkeypatch):
    """Substitui o COPY e devolve as linhas que o adaptador entregou, por arquivo."""
    entregues: list[list[tuple]] = []

    def falso(conn, schema, table, columns, rows):
        lote = [tuple(r) for r in rows]
        assert table == "dados"
        assert columns[4] == "modificacao"
        entregues.append(lote)
        return len(lote)

    monkeypatch.setattr(sidra_mod, "copy_from_rows", falso)
    return entregues


@pytest.fixture
def bruto(tmp_path) -> Path:
    """Raiz do bruto do ibge-sidra com versões, sidecar e arquivos que não são dados."""
    raiz = tmp_path / "ibge-sidra"
    _grava(
        raiz,
        "t-1086_p-202401_f-a_n1-all_v-all@20240301.json",
        [_linha("1.0", "202401")],
    )
    _grava(
        raiz,
        "t-1086_p-202401_f-a_n1-all_v-all@20250611.json",
        [_linha("2.0", "202401")],
        sidecar_modificacao="2025-06-11",
    )
    _grava(
        raiz,
        "t-1086_p-202402_f-a_n1-all_v-all@00010101.json",
        [_linha("3.0", "202402")],
    )
    (raiz / "t-1086" / "metadados@20261124.json").write_text("{}", encoding="utf-8")
    _grava(
        raiz,
        "t-2296_p-202401_f-a_n1-all_v-all@20250102.json",
        [_linha("4.0", "202401")],
        tabela="2296",
    )
    (raiz / "indice_pesquisas@20261124.json").write_text("[]", encoding="utf-8")
    return raiz


def _por_valor(entregues: list[list[tuple]]) -> dict[str, tuple]:
    return {row[6]: row for lote in entregues for row in lote}


# --------------------------------------------------------------------------
# Hipótese 1: pasta + --all
# --------------------------------------------------------------------------
def test_pasta_com_all_carrega_so_a_versao_vigente_de_cada_arquivo(bruto, capturado):
    state = MockDbState()
    conn = state.make_conn()
    adapter = get_adapter("sidra")

    total = adapter.load(conn, bruto, all_partitions=True)

    assert total == 3
    nomes = sorted(r[0] for r in state.recorded)
    assert nomes == [
        "t-1086_p-202401_f-a_n1-all_v-all@20250611.json",
        "t-1086_p-202402_f-a_n1-all_v-all@00010101.json",
        "t-2296_p-202401_f-a_n1-all_v-all@20250102.json",
    ]
    por_tabela = {r[0].split("_")[0]: r[2] for r in state.recorded}
    assert por_tabela == {"t-1086": "1086", "t-2296": "2296"}
    assert set(_por_valor(capturado)) == {"2.0", "3.0", "4.0"}  # sem a versão antiga


def test_pasta_de_uma_tabela_tambem_funciona(bruto, capturado):
    state = MockDbState()
    total = get_adapter("sidra").load(
        state.make_conn(), bruto / "t-1086", all_partitions=True
    )
    assert total == 2
    assert {r[2] for r in state.recorded} == {"1086"}


def test_pasta_sem_all_da_erro_claro_e_nao_carrega(bruto, capturado):
    state = MockDbState()
    with pytest.raises(ValueError, match="--all"):
        get_adapter("sidra").load(state.make_conn(), bruto)
    assert state.recorded == []
    assert capturado == []


def test_pasta_com_table_carrega_so_a_tabela(bruto, capturado):
    state = MockDbState()
    total = get_adapter("sidra").load(state.make_conn(), bruto, table="2296")
    assert total == 1
    assert [r[2] for r in state.recorded] == ["2296"]


def test_pasta_sem_arquivo_de_dados_da_erro_claro(tmp_path, capturado):
    (tmp_path / "t-1").mkdir()
    (tmp_path / "t-1" / "metadados@20261124.json").write_text("{}")
    with pytest.raises(ValueError, match="nenhum arquivo de dados"):
        get_adapter("sidra").load(
            MockDbState().make_conn(), tmp_path, all_partitions=True
        )


def test_pasta_recarregada_nao_duplica(bruto, capturado):
    state = MockDbState()
    conn = state.make_conn()
    adapter = get_adapter("sidra")
    assert adapter.load(conn, bruto, all_partitions=True) == 3
    assert adapter.load(conn, bruto, all_partitions=True) == 0
    assert len(state.recorded) == 3


def test_cli_passa_all_ao_adaptador(bruto, capturado, monkeypatch):
    from typer.testing import CliRunner

    from quantilica_sql import cli

    state = MockDbState()
    conn = state.make_conn()

    class _Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(cli, "get_connection", lambda **kw: _Ctx())
    runner = CliRunner()
    ok = runner.invoke(cli.app, ["load", "sidra", "-s", str(bruto), "--all"])
    assert ok.exit_code == 0, ok.output
    assert len(state.recorded) == 3

    falha = runner.invoke(cli.app, ["load", "sidra", "-s", str(bruto)])
    assert falha.exit_code == 1
    assert "--all" in falha.output


# --------------------------------------------------------------------------
# Hipótese 2: data da fonte
# --------------------------------------------------------------------------
def test_data_vem_do_sidecar(bruto, capturado):
    get_adapter("sidra").load(MockDbState().make_conn(), bruto, all_partitions=True)
    assert _por_valor(capturado)["2.0"][4] == dt.date(2025, 6, 11)


def test_data_vem_do_carimbo_do_nome_sem_sidecar(bruto, capturado):
    get_adapter("sidra").load(MockDbState().make_conn(), bruto, all_partitions=True)
    assert _por_valor(capturado)["4.0"][4] == dt.date(2025, 1, 2)


def test_sem_data_na_fonte_fica_nula_e_nunca_um_valor_fixo(bruto, capturado):
    get_adapter("sidra").load(MockDbState().make_conn(), bruto, all_partitions=True)
    assert _por_valor(capturado)["3.0"][4] is None  # @00010101: a API não informou
    assert all(
        row[4] != "2026-01-01" for lote in capturado for row in lote
    )  # o padrão fixo antigo


def test_sidecar_tem_precedencia_sobre_o_carimbo(tmp_path, capturado):
    arq = _grava(
        tmp_path,
        "t-1086_p-202401_f-a_n1-all_v-all@20250611.json",
        [_linha("9.0", "202401")],
        sidecar_modificacao="2025-06-10",
    )
    get_adapter("sidra").load(MockDbState().make_conn(), arq)
    assert _por_valor(capturado)["9.0"][4] == dt.date(2025, 6, 10)


def test_sidecar_sem_data_cai_no_carimbo(tmp_path, capturado):
    arq = _grava(
        tmp_path,
        "t-1086_p-202401_f-a_n1-all_v-all@20250611.json",
        [_linha("9.0", "202401")],
        sidecar_modificacao="0001-01-01",
    )
    get_adapter("sidra").load(MockDbState().make_conn(), arq)
    assert _por_valor(capturado)["9.0"][4] == dt.date(2025, 6, 11)


def test_arquivo_unico_usa_o_id_da_tabela_do_nome(tmp_path, capturado):
    arq = _grava(
        tmp_path,
        "t-1086_p-202401_f-a_n1-all_v-all@20250611.json",
        [_linha("9.0", "202401")],
    )
    state = MockDbState()
    get_adapter("sidra").load(state.make_conn(), arq)
    assert state.recorded[0][2] == "1086"
    assert _por_valor(capturado)["9.0"][0] == "1086"


def test_campo_modificacao_do_proprio_item_vale_mais_que_o_arquivo(tmp_path, capturado):
    item = _linha("9.0", "202401") | {"modificacao": "2024-12-31"}
    arq = _grava(tmp_path, "t-1086_p-202401_f-a_n1-all_v-all@20250611.json", [item])
    get_adapter("sidra").load(MockDbState().make_conn(), arq)
    assert _por_valor(capturado)["9.0"][4] == dt.date(2024, 12, 31)


def test_modificacao_invalida_no_item_falha_com_erro_claro(tmp_path, capturado):
    item = _linha("9.0", "202401") | {"modificacao": "ontem"}
    arq = _grava(tmp_path, "t-1086_p-202401_f-a_n1-all_v-all@20250611.json", [item])
    with pytest.raises(ValueError, match="modificacao"):
        get_adapter("sidra").load(MockDbState().make_conn(), arq)


def test_linha_de_5_campos_recebe_a_data_do_arquivo(tmp_path, capturado):
    arq = tmp_path / "t-1086_p-202401_f-a_n1-all_v-all@20250611.json"
    arq.write_text(json.dumps([["1086", 1, 2, 202401, "7.5"]]), encoding="utf-8")
    get_adapter("sidra").load(MockDbState().make_conn(), arq)
    assert _por_valor(capturado)["7.5"][4] == dt.date(2025, 6, 11)


# --------------------------------------------------------------------------
# Integração com PostgreSQL real
# --------------------------------------------------------------------------
URL = os.environ.get("QUANTILICA_TEST_DATABASE_URL")


@pytest.fixture
def pg():
    psycopg = pytest.importorskip("psycopg")
    from psycopg import sql

    schema = f"qsql_sidra_{uuid.uuid4().hex[:8]}"

    class _Sidra(SidraAdapter):
        @property
        def schema(self) -> str:
            return schema

    conn = psycopg.connect(URL, autocommit=True)
    yield _Sidra(), conn, schema
    conn.execute(
        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
    )
    conn.close()


@pytest.mark.skipif(not URL, reason="QUANTILICA_TEST_DATABASE_URL ausente")
def test_pg_pasta_all_grava_data_da_fonte_e_nula_sem_data(bruto, pg):
    adapter, conn, schema = pg
    assert adapter.load(conn, bruto, all_partitions=True) == 3
    rows = conn.execute(
        f'SELECT tabela_sidra_id, periodo_id, modificacao, v FROM "{schema}".dados '
        "ORDER BY v"
    ).fetchall()
    assert rows == [
        ("1086", 202401, dt.date(2025, 6, 11), "2.0"),
        ("1086", 202402, None, "3.0"),
        ("2296", 202401, dt.date(2025, 1, 2), "4.0"),
    ]
    assert adapter.load(conn, bruto, all_partitions=True) == 0  # idempotente


@pytest.mark.skipif(not URL, reason="QUANTILICA_TEST_DATABASE_URL ausente")
def test_pg_tabela_antiga_com_not_null_e_default_e_migrada(bruto, pg):
    adapter, conn, schema = pg
    conn.execute(f'CREATE SCHEMA "{schema}"')
    conn.execute(
        f"""CREATE TABLE "{schema}".dados (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            tabela_sidra_id TEXT NOT NULL, localidade_id BIGINT NOT NULL,
            dimensao_id BIGINT NOT NULL, periodo_id INTEGER NOT NULL,
            modificacao DATE NOT NULL DEFAULT CURRENT_DATE,
            ativo BOOLEAN NOT NULL DEFAULT TRUE, v TEXT NOT NULL)"""
    )
    assert adapter.load(conn, bruto, all_partitions=True) == 3
    nulos = conn.execute(
        f'SELECT count(*) FROM "{schema}".dados WHERE modificacao IS NULL'
    ).fetchone()
    assert nulos == (1,)
