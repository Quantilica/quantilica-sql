"""BcbAdapter: pasta do bruto (``--all``/``--table``), ledger, revisão e zero.

Os testes de entrada usam um banco simulado (``MockDbState``); os de integração
exigem ``QUANTILICA_TEST_DATABASE_URL`` (banco descartável) e usam um schema
próprio, removido ao final.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
from test_adapters_and_core import MockDbState

from quantilica_sql.adapters import get_adapter
from quantilica_sql.adapters.bcb import BcbAdapter

SNAP_ANTIGO = "series_433@20261118T080000.json"
SNAP_20_11 = "series_433@20261120T090000.json"
SNAP_24_11 = "series_433@20261124T090000.json"


def _grava(raiz: Path, nome: str, pontos: list[dict], *, pasta: str = "data") -> Path:
    alvo = raiz / pasta
    alvo.mkdir(parents=True, exist_ok=True)
    arq = alvo / nome
    arq.write_text(json.dumps(pontos), encoding="utf-8")
    return arq


def _ponto(data: str, valor: str) -> dict[str, str]:
    return {"data": data, "valor": valor}


# Snapshots do ensaio: 20/11 (0,44 em set e 0,50 em out) e 24/11 (set revisto para
# 0,45, out igual e ponto novo em nov).
DADOS_20_11 = [_ponto("01/09/2026", "0.44"), _ponto("01/10/2026", "0.50")]
DADOS_24_11 = [
    _ponto("01/09/2026", "0.45"),
    _ponto("01/10/2026", "0.50"),
    _ponto("01/11/2026", "0.39"),
]


@pytest.fixture
def bruto(tmp_path) -> Path:
    """Raiz do bruto: ``data/`` com dois snapshots da 433 (o de 20/11 é o vigente,
    há um mais antigo), uma outra série, sidecar, ``*.tmp`` e os metadados."""
    raiz = tmp_path / "bcb-sgs"
    _grava(raiz, SNAP_ANTIGO, [_ponto("01/09/2026", "0.10")])
    _grava(raiz, SNAP_20_11, DADOS_20_11)
    _grava(raiz, "series_11@20261120T090000.json", [_ponto("01/09/2026", "13.65")])
    (raiz / "data" / (SNAP_20_11 + ".manifest.json")).write_text("{}", "utf-8")
    (raiz / "data" / "series_99@20261120T090000.json.tmp").write_text("[", "utf-8")
    meta = raiz / "bcb-sgs_2026-11" / "metadata"
    meta.mkdir(parents=True)
    (meta / "000433.json").write_text('{"nome": "Taxa"}', "utf-8")
    return raiz


# --------------------------------------------------------------------------
# Entrada: arquivo, pasta data/, raiz do bruto, --all e --table
# --------------------------------------------------------------------------
def test_entrada_raiz_com_all_carrega_so_o_snapshot_vigente_de_cada_serie(bruto):
    state = MockDbState()
    total = get_adapter("bcb").load(state.make_conn(rowcount=2), bruto, all=True)
    nomes = sorted(r[0] for r in state.recorded)
    assert nomes == ["series_11@20261120T090000.json", SNAP_20_11]
    assert {r[2] for r in state.recorded} == {"433", "11"}
    assert total == 4  # 2 chamadas de rowcount=2 (mock)


def test_entrada_pasta_data_com_all_tambem_funciona(bruto):
    state = MockDbState()
    get_adapter("bcb").load(state.make_conn(), bruto / "data", all_partitions=True)
    assert sorted(r[0] for r in state.recorded) == [
        "series_11@20261120T090000.json",
        SNAP_20_11,
    ]


def test_entrada_pasta_sem_all_nem_table_da_erro_claro_e_nao_abre_como_arquivo(bruto):
    state = MockDbState()
    with pytest.raises(ValueError, match="--all"):
        get_adapter("bcb").load(state.make_conn(), bruto)
    assert state.recorded == []


def test_entrada_table_carrega_so_a_serie(bruto):
    state = MockDbState()
    get_adapter("bcb").load(state.make_conn(), bruto, table="series_433")
    assert [r[2] for r in state.recorded] == ["433"]
    state = MockDbState()
    get_adapter("bcb").load(state.make_conn(), bruto, table="000433")
    assert [r[0] for r in state.recorded] == [SNAP_20_11]


def test_entrada_table_sem_correspondencia_da_erro_claro(bruto):
    with pytest.raises(ValueError, match="nenhum arquivo de dados da série 7"):
        get_adapter("bcb").load(MockDbState().make_conn(), bruto, table="7")


def test_entrada_pasta_sem_arquivo_de_dados_da_erro_claro(tmp_path):
    (tmp_path / "bcb-sgs_2026-11" / "metadata").mkdir(parents=True)
    (tmp_path / "bcb-sgs_2026-11" / "metadata" / "000433.json").write_text("{}")
    with pytest.raises(ValueError, match="nenhum arquivo de dados do BCB SGS"):
        get_adapter("bcb").load(MockDbState().make_conn(), tmp_path, all=True)


def test_entrada_arquivo_unico_usa_o_id_do_nome(bruto):
    state = MockDbState()
    get_adapter("bcb").load(state.make_conn(), bruto / "data" / SNAP_20_11)
    assert [r[2] for r in state.recorded] == ["433"]


def test_bcb_sgs_e_alias_do_adaptador_bcb():
    """``load bcb-sgs`` resolve para o mesmo adaptador (domínio ``bcb``, schema
    ``bcb_sgs``) e o alias aparece na lista de domínios da CLI."""
    from quantilica_sql.adapters import list_supported_domains
    from quantilica_sql.adapters.bcb import BcbAdapter

    for nome in ("bcb", "bcb-sgs", " BCB-SGS "):
        adapter = get_adapter(nome)
        assert isinstance(adapter, BcbAdapter)
        assert (adapter.domain, adapter.schema) == ("bcb", "bcb_sgs")
    assert "bcb-sgs" in list_supported_domains()


def test_entrada_arquivo_inexistente_falha_em_vez_de_registrar_zero_linhas(tmp_path):
    state = MockDbState()
    with pytest.raises(FileNotFoundError):
        get_adapter("bcb").load(state.make_conn(), tmp_path / "series_433@2026.json")
    assert state.recorded == []


@pytest.mark.parametrize("dominio", ["bcb", "bcb-sgs"])
def test_entrada_cli_com_pasta_e_all_sai_com_zero(bruto, monkeypatch, dominio):
    """``bcb-sgs`` (o nome da fonte e o que a documentação usa) é alias de ``bcb``."""
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
    ok = runner.invoke(cli.app, ["load", dominio, "-s", str(bruto), "--all"])
    assert ok.exit_code == 0, ok.output
    assert len(state.recorded) == 2
    falha = runner.invoke(cli.app, ["load", dominio, "-s", str(bruto)])
    assert falha.exit_code == 1
    assert "--all" in falha.output


# --------------------------------------------------------------------------
# Integração com PostgreSQL real
# --------------------------------------------------------------------------
URL = os.environ.get("QUANTILICA_TEST_DATABASE_URL")
pg_only = pytest.mark.skipif(not URL, reason="QUANTILICA_TEST_DATABASE_URL ausente")


@pytest.fixture
def pg():
    psycopg = pytest.importorskip("psycopg")
    from psycopg import sql

    schema = f"qsql_bcb_{uuid.uuid4().hex[:8]}"

    class _Bcb(BcbAdapter):
        @property
        def schema(self) -> str:
            return schema

    conn = psycopg.connect(URL, autocommit=True)
    yield _Bcb(), conn, schema
    conn.execute(
        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
    )
    conn.close()


def _linhas(conn, schema: str, serie: int = 433) -> list[tuple]:
    return conn.execute(
        f'SELECT date, value, ativo FROM "{schema}".series_data '
        "WHERE series_id = %s ORDER BY date, ativo",
        (serie,),
    ).fetchall()


@pg_only
def test_pg_entrada_pasta_all_carrega_o_snapshot_de_20_11(bruto, pg):
    adapter, conn, schema = pg
    assert adapter.load(conn, bruto, all_partitions=True) == 3  # 2 da 433 + 1 da 11
    assert _linhas(conn, schema) == [
        (dt.date(2026, 9, 1), Decimal("0.44"), True),
        (dt.date(2026, 10, 1), Decimal("0.50"), True),
    ]
    assert _linhas(conn, schema, 11) == [(dt.date(2026, 9, 1), Decimal("13.65"), True)]
    assert adapter.load(conn, bruto, all_partitions=True) == 0  # idempotente


@pg_only
def test_pg_entrada_cli_real_sai_com_zero(bruto, pg, monkeypatch):
    from typer.testing import CliRunner

    from quantilica_sql import cli

    adapter, conn, schema = pg
    monkeypatch.setattr(cli, "get_adapter", lambda domain: adapter)

    class _Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(cli, "get_connection", lambda **kw: _Ctx())
    res = CliRunner().invoke(cli.app, ["load", "bcb", "-s", str(bruto), "--all"])
    assert res.exit_code == 0, res.output
    assert len(_linhas(conn, schema)) == 2


@pg_only
def test_pg_ledger_igual_pula_hash_novo_recarrega_e_force_recarrega(tmp_path, pg):
    adapter, conn, schema = pg
    arq = _grava(tmp_path, SNAP_20_11, DADOS_20_11)
    assert adapter.load(conn, arq) == 2
    ledger = f'SELECT sha256, linhas FROM "{schema}".arquivo_carregado'
    antes = conn.execute(ledger).fetchall()
    assert len(antes) == 1 and antes[0][0] is not None

    # igual: pula (0 linhas, ledger inalterado)
    assert adapter.load(conn, arq) == 0
    assert conn.execute(ledger).fetchall() == antes

    # mesmo nome, conteúdo (hash) novo: recarrega e aplica a revisão
    arq.write_text(json.dumps(DADOS_24_11), encoding="utf-8")
    assert adapter.load(conn, arq) == 2  # a revisão de set e o ponto novo de nov
    depois = conn.execute(ledger).fetchall()
    assert depois != antes and depois[0][0] != antes[0][0]

    # --force: recarrega o mesmo arquivo; reparo de valor adulterado no banco
    conn.execute(
        f'UPDATE "{schema}".series_data SET value = 9 '
        "WHERE series_id = 433 AND date = '2026-10-01' AND ativo"
    )
    assert adapter.load(conn, arq) == 0  # sem --force: hash igual, pula
    assert adapter.load(conn, arq, force=True) == 1  # o ponto adulterado volta
    ativos = [r for r in _linhas(conn, schema) if r[2]]
    assert [r[1] for r in ativos] == [Decimal("0.45"), Decimal("0.50"), Decimal("0.39")]


@pg_only
def test_pg_ledger_legado_sem_hash_so_ganha_o_hash(tmp_path, pg):
    adapter, conn, schema = pg
    arq = _grava(tmp_path, SNAP_20_11, DADOS_20_11)
    assert adapter.load(conn, arq) == 2
    conn.execute(f'UPDATE "{schema}".arquivo_carregado SET sha256 = NULL')
    assert adapter.load(conn, arq) == 0
    (sha,) = conn.execute(f'SELECT sha256 FROM "{schema}".arquivo_carregado').fetchone()
    assert sha is not None
    assert len(_linhas(conn, schema)) == 2


@pg_only
def test_pg_revisao_desativa_o_valor_antigo_e_insere_o_novo(tmp_path, pg):
    adapter, conn, schema = pg
    primeiro = _grava(tmp_path, SNAP_20_11, DADOS_20_11)
    segundo = _grava(tmp_path, SNAP_24_11, DADOS_24_11)
    assert adapter.load(conn, primeiro) == 2
    assert adapter.load(conn, segundo) == 2  # 0,45 de set e o ponto novo de nov
    assert _linhas(conn, schema) == [
        (dt.date(2026, 9, 1), Decimal("0.44"), False),
        (dt.date(2026, 9, 1), Decimal("0.45"), True),
        (dt.date(2026, 10, 1), Decimal("0.50"), True),  # idêntico: não mexe
        (dt.date(2026, 11, 1), Decimal("0.39"), True),
    ]
    ativas = conn.execute(
        f'SELECT count(*) FROM "{schema}".series_data WHERE ativo'
    ).fetchone()
    assert ativas == (3,)
    info = adapter.status(conn)
    assert info["series_data_ativas"] == 3
    assert info["series_data_revisadas"] == 1
    assert info["series_distintas"] == 1
    assert info["series_data_rows"] == 4
    assert info["series_carregadas"] == 2


@pg_only
def test_pg_revisao_recarga_identica_e_no_op_sem_churn(tmp_path, pg):
    adapter, conn, schema = pg
    a = _grava(tmp_path, SNAP_20_11, DADOS_20_11)
    b = _grava(tmp_path, "series_433@20261121T090000.json", DADOS_20_11)
    assert adapter.load(conn, a) == 2
    assert adapter.load(conn, b) == 0  # outro arquivo, mesmos valores: nada muda
    assert len(_linhas(conn, schema)) == 2
    assert all(r[2] for r in _linhas(conn, schema))


@pg_only
def test_pg_revisao_pasta_all_pega_so_o_ultimo_e_recarga_da_pasta_nao_duplica(
    tmp_path, pg
):
    adapter, conn, schema = pg
    raiz = tmp_path / "bcb-sgs"
    _grava(raiz, SNAP_20_11, DADOS_20_11)
    assert adapter.load(conn, raiz, all=True) == 2
    _grava(raiz, SNAP_24_11, DADOS_24_11)
    assert adapter.load(conn, raiz, all=True) == 2  # só o de 24/11 entra
    assert adapter.load(conn, raiz, all=True) == 0
    assert sum(1 for r in _linhas(conn, schema) if r[2]) == 3


@pg_only
def test_pg_ponto_repetido_no_arquivo_vale_o_ultimo_sem_violar_o_indice(tmp_path, pg):
    adapter, conn, schema = pg
    arq = _grava(
        tmp_path,
        SNAP_20_11,
        [_ponto("01/09/2026", "0.40"), _ponto("01/09/2026", "0.44")],
    )
    assert adapter.load(conn, arq) == 1
    assert _linhas(conn, schema) == [(dt.date(2026, 9, 1), Decimal("0.44"), True)]


@pg_only
def test_pg_zero_numerico_e_carregado_como_zero(tmp_path, pg):
    adapter, conn, schema = pg
    arq = tmp_path / "series_432@20261120T090000.json"
    arq.write_text(
        '[{"data": "01/09/2026", "valor": 0},'
        ' {"data": "01/10/2026", "valor": 0.0},'
        ' {"data": "01/11/2026", "valor": "0.00"},'
        ' {"data": "01/12/2026", "valor": null},'
        ' {"data": "01/01/2027", "valor": ""}]',
        encoding="utf-8",
    )
    assert adapter.load(conn, arq) == 5
    assert [(r[0], r[1]) for r in _linhas(conn, schema, 432)] == [
        (dt.date(2026, 9, 1), Decimal("0")),
        (dt.date(2026, 10, 1), Decimal("0.0")),
        (dt.date(2026, 11, 1), Decimal("0.00")),
        (dt.date(2026, 12, 1), None),
        (dt.date(2027, 1, 1), None),
    ]


@pg_only
def test_pg_zero_em_value_e_valor_do_formato_do_banco(tmp_path, pg):
    adapter, conn, schema = pg
    arq = tmp_path / "series_5@20261120T090000.json"
    arq.write_text(json.dumps([{"date": "2026-09-01", "value": 0}]), encoding="utf-8")
    assert adapter.load(conn, arq) == 1
    assert _linhas(conn, schema, 5) == [(dt.date(2026, 9, 1), Decimal("0"), True)]


@pg_only
def test_pg_atomico_erro_no_meio_deixa_banco_e_ledger_como_estavam(tmp_path, pg):
    adapter, conn, schema = pg
    ok = _grava(tmp_path, SNAP_20_11, DADOS_20_11)
    assert adapter.load(conn, ok) == 2
    ledger = f'SELECT count(*) FROM "{schema}".arquivo_carregado'
    linhas_antes = _linhas(conn, schema)
    ruim = _grava(tmp_path, SNAP_24_11, DADOS_24_11)

    import quantilica_sql.adapters.bcb as bcb_mod

    original = bcb_mod.copy_from_rows

    def falha(*a, **kw):
        original(*a, **kw)
        raise RuntimeError("falha injetada depois do COPY")

    bcb_mod.copy_from_rows = falha
    try:
        with pytest.raises(RuntimeError, match="falha injetada"):
            adapter.load(conn, ruim)
    finally:
        bcb_mod.copy_from_rows = original
    assert conn.execute(ledger).fetchone() == (1,)
    assert _linhas(conn, schema) == linhas_antes
