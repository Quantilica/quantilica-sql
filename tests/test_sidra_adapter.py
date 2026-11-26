"""SidraAdapter: entrada (arquivo, pasta, ``--all``, ``--table``, ``--historico``),
data da fonte, CLI e esquema antigo.

Os testes de carga em si (vigência, versões, as-of) estão em ``test_sidra_carga.py``.
Estes usam as fixtures reais de ``tests/data/sidra`` e exigem
``QUANTILICA_TEST_DATABASE_URL`` (banco descartável) quando carregam.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest
from _sidra_fixtures import (
    arquivo_de_dados,
    copia_tabelas,
    nova_versao,
)

from quantilica_sql.adapters import get_adapter
from quantilica_sql.adapters import sidra as sidra_mod
from quantilica_sql.adapters.sidra import SidraAdapter, modificacao_do_arquivo

TABELAS_DO_BRUTO = ("1086", "1737")


@pytest.fixture
def bruto(tmp_path):
    """Raiz do bruto do ibge-sidra: versões, sidecar, ``@00010101`` e auxiliares."""
    raiz = copia_tabelas(tmp_path / "ibge-sidra", *TABELAS_DO_BRUTO)
    v1737 = arquivo_de_dados(raiz, "1737")
    # versão antiga da mesma consulta (sem sidecar: a data vem do carimbo)
    nova_versao(v1737, "20240301", lambda r: {**r, "V": "1.0"})
    # outra consulta, sem data na fonte
    nova_versao(v1737, "00010101", sufixo="_sd")
    (raiz / "t-1737" / "metadados@20261001.json").write_text("{}", encoding="utf-8")
    (raiz / "indice_pesquisas@20261124.json").write_text("[]", encoding="utf-8")
    (raiz / "t-1737" / "x@20261124.json.tmp").write_text("[", encoding="utf-8")
    return raiz


# --------------------------------------------------------------------------
# Entrada: pasta, --all, --table, --historico (sem banco)
# --------------------------------------------------------------------------
def _nomes(arquivos):
    return sorted(
        a.name.split("@")[0][-8:] + "@" + a.name.split("@")[1][:8] for a in arquivos
    )


def test_pasta_com_all_so_a_versao_vigente_de_cada_consulta(bruto):
    arquivos = SidraAdapter()._arquivos_vigentes(bruto, None)
    nomes = sorted(a.name for a in arquivos)
    assert len(nomes) == 3
    assert not any("20240301" in n for n in nomes)  # a versão antiga fica de fora
    assert not any(n.startswith(("metadados", "indice")) for n in nomes)
    assert not any(n.endswith((".manifest.json", ".tmp")) for n in nomes)


def test_pasta_de_uma_tabela_tambem_funciona(bruto):
    arquivos = SidraAdapter()._arquivos_vigentes(bruto / "t-1737", None)
    assert {a.parent.name for a in arquivos} == {"t-1737"}
    assert len(arquivos) == 2


def test_pasta_sem_all_da_erro_claro(bruto):
    with pytest.raises(ValueError, match="--all"):
        SidraAdapter()._arquivos_vigentes(bruto, None, all_partitions=False)


def test_pasta_sem_all_nem_table_nao_carrega_nem_cria_esquema(bruto, pg):
    adapter, conn, schema = pg
    with pytest.raises(ValueError, match="--all"):
        adapter.load(conn, bruto)
    assert conn.execute(
        "SELECT count(*) FROM pg_namespace WHERE nspname = %s", (schema,)
    ).fetchone() == (0,)


def test_pasta_com_table_carrega_so_a_tabela(bruto):
    arquivos = SidraAdapter()._arquivos_vigentes(bruto, "1086", all_partitions=False)
    assert [a.parent.name for a in arquivos] == ["t-1086"]
    com_prefixo = SidraAdapter()._arquivos_vigentes(
        bruto, "t-1086", all_partitions=False
    )
    assert com_prefixo == arquivos


def test_pasta_com_table_inexistente_da_erro_claro(bruto):
    with pytest.raises(ValueError, match="tabela 9999"):
        SidraAdapter()._arquivos_vigentes(bruto, "9999", all_partitions=False)


def test_pasta_sem_arquivo_de_dados_da_erro_claro(tmp_path):
    (tmp_path / "t-1").mkdir()
    (tmp_path / "t-1" / "metadados@20261124.json").write_text("{}")
    with pytest.raises(ValueError, match="nenhum arquivo de dados"):
        SidraAdapter()._arquivos_vigentes(tmp_path, None)


def test_historico_devolve_todas_as_versoes_da_mais_antiga_para_a_mais_nova(bruto):
    arquivos = SidraAdapter()._arquivos_vigentes(bruto, None, historico=True)
    assert len(arquivos) == 4  # as 3 vigentes + a versão de 2024
    datas = [modificacao_do_arquivo(a) for a in arquivos]
    sem_data = [d for d in datas if d is None]
    com_data = [d for d in datas if d is not None]
    assert datas[: len(sem_data)] == sem_data  # sem data é a mais antiga
    assert com_data == sorted(com_data)
    assert dt.date(2024, 3, 1) in com_data


def test_historico_sem_all_nem_table_tambem_pede_um_dos_dois(bruto):
    with pytest.raises(ValueError, match="--all"):
        SidraAdapter()._arquivos_vigentes(
            bruto, None, all_partitions=False, historico=True
        )


def test_historico_com_table_limita_a_tabela(bruto):
    arquivos = SidraAdapter()._arquivos_vigentes(
        bruto, "1737", all_partitions=False, historico=True
    )
    assert {a.parent.name for a in arquivos} == {"t-1737"}
    assert len(arquivos) == 3


# --------------------------------------------------------------------------
# Entrada com banco
# --------------------------------------------------------------------------
def test_pasta_com_all_carrega_so_a_vigente(bruto, pg):
    adapter, conn, schema = pg
    total = adapter.load(conn, bruto, all_partitions=True)
    versoes = [
        r[0] for r in conn.execute(f'SELECT arquivo FROM "{schema}".versao ORDER BY 1')
    ]
    assert len(versoes) == 3
    assert not any("20240301" in v for v in versoes)
    assert (
        total
        == conn.execute(f'SELECT count(*) FROM "{schema}".observacao').fetchone()[0]
    )


def test_pasta_recarregada_nao_duplica(bruto, pg):
    adapter, conn, schema = pg
    primeira = adapter.load(conn, bruto, all_partitions=True)
    assert primeira > 0
    assert adapter.load(conn, bruto, all_partitions=True) == 0
    assert conn.execute(f'SELECT count(*) FROM "{schema}".versao').fetchone() == (3,)


def test_table_carrega_so_a_tabela(bruto, pg):
    adapter, conn, schema = pg
    adapter.load(conn, bruto, table="1086")
    assert conn.execute(
        f'SELECT DISTINCT tabela_id FROM "{schema}".observacao'
    ).fetchall() == [(1086,)]


def test_historico_carrega_todas_as_versoes(bruto, pg):
    adapter, conn, schema = pg
    adapter.load(conn, bruto, all_partitions=True, historico=True)
    assert conn.execute(f'SELECT count(*) FROM "{schema}".versao').fetchone() == (4,)


def test_arquivo_unico_usa_o_id_da_tabela_do_nome(bruto, pg):
    adapter, conn, schema = pg
    adapter.load(conn, arquivo_de_dados(bruto, "1086"))
    assert conn.execute(
        f'SELECT DISTINCT tabela_id FROM "{schema}".versao'
    ).fetchall() == [(1086,)]
    assert conn.execute(
        f'SELECT particao FROM "{schema}".arquivo_carregado'
    ).fetchall() == [("1086",)]


def test_arquivo_inexistente_e_sem_source_path_dao_erro_claro(tmp_path, pg):
    adapter, conn, _ = pg
    with pytest.raises(ValueError, match="não existe"):
        adapter.load(conn, tmp_path / "nada.json")
    with pytest.raises(ValueError, match="source-path"):
        adapter.load(conn, None)


def test_nome_diz_uma_tabela_e_os_metadados_outra_da_erro(bruto, pg):
    from quantilica_sql.sidra import SidraFonteError

    adapter, conn, _ = pg
    arq = arquivo_de_dados(bruto, "1086")
    (bruto / "t-1086" / "metadados@20261125.json").write_text(
        (bruto / "t-1737" / "metadados@20261125.json").read_text(), encoding="utf-8"
    )
    with pytest.raises(SidraFonteError):
        adapter.load(conn, arq)


# --------------------------------------------------------------------------
# Data da fonte
# --------------------------------------------------------------------------
def test_data_vem_do_sidecar(bruto):
    real = next((bruto / "t-1737").glob("*@20261112.json"))
    assert modificacao_do_arquivo(real) == dt.date(2026, 11, 12)


def test_data_vem_do_carimbo_do_nome_sem_sidecar(bruto):
    antigo = next(p for p in (bruto / "t-1737").glob("*@20240301.json"))
    assert modificacao_do_arquivo(antigo) == dt.date(2024, 3, 1)


def test_sem_data_na_fonte_fica_nula_e_nunca_um_valor_fixo(bruto, pg):
    adapter, conn, schema = pg
    sem_data = next(p for p in (bruto / "t-1737").glob("*@00010101.json"))
    assert modificacao_do_arquivo(sem_data) is None
    adapter.load(conn, sem_data)
    assert conn.execute(f'SELECT modificacao FROM "{schema}".versao').fetchall() == [
        (None,)
    ]
    assert conn.execute(
        f'SELECT count(*) FROM "{schema}".dados WHERE modificacao IS NULL'
    ).fetchone() == (6,)


def test_sidecar_tem_precedencia_sobre_o_carimbo(tmp_path):
    arq = tmp_path / "t-1086_p-202401_f-a_n1-all_v-all@20250611.json"
    arq.write_text("[]")
    sidecar = arq.with_name(arq.name + ".manifest.json")
    sidecar.write_text(json.dumps({"metadata": {"modificacao": "2025-06-10"}}))
    assert modificacao_do_arquivo(arq) == dt.date(2025, 6, 10)


def test_sidecar_sem_data_cai_no_carimbo(tmp_path):
    arq = tmp_path / "t-1086_p-202401_f-a_n1-all_v-all@20250611.json"
    arq.write_text("[]")
    arq.with_name(arq.name + ".manifest.json").write_text(
        json.dumps({"metadata": {"modificacao": "0001-01-01"}})
    )
    assert modificacao_do_arquivo(arq) == dt.date(2025, 6, 11)


def test_modificacao_invalida_falha_com_erro_claro(tmp_path):
    arq = tmp_path / "t-1086_p-202401_f-a_n1-all_v-all@20250611.json"
    arq.write_text("[]")
    arq.with_name(arq.name + ".manifest.json").write_text(
        json.dumps({"metadata": {"modificacao": "ontem"}})
    )
    with pytest.raises(ValueError, match="modificacao"):
        modificacao_do_arquivo(arq)


def test_modificacao_do_arquivo_aceita_os_formatos_da_fonte():
    parse = sidra_mod._parse_modificacao
    assert parse("20250611", origem="x") == dt.date(2025, 6, 11)
    assert parse("11/06/2025", origem="x") == dt.date(2025, 6, 11)
    assert parse("2025-06-11T10:00:00", origem="x") == dt.date(2025, 6, 11)
    assert parse("0001-01-01", origem="x") is None
    assert parse(None, origem="x") is None


# --------------------------------------------------------------------------
# Banco: data da fonte e idempotência
# --------------------------------------------------------------------------
def test_pg_pasta_all_grava_data_da_fonte_e_nula_sem_data(bruto, pg):
    adapter, conn, schema = pg
    adapter.load(conn, bruto, all_partitions=True)
    rows = conn.execute(
        f'SELECT DISTINCT tabela_sidra_id, modificacao FROM "{schema}".dados '
        "ORDER BY 1, 2 NULLS FIRST"
    ).fetchall()
    assert rows == [
        ("1086", dt.date(2026, 9, 15)),
        ("1737", None),
        ("1737", dt.date(2026, 11, 12)),
    ]


# --------------------------------------------------------------------------
# CLI e esquema antigo
# --------------------------------------------------------------------------
@pytest.fixture
def cli_com_banco(pg, monkeypatch):
    from quantilica_sql import cli

    adapter, conn, schema = pg
    monkeypatch.setattr(SidraAdapter, "schema", property(lambda self: schema))

    class _Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(cli, "get_connection", lambda **kw: _Ctx())
    return cli, conn, schema


def test_cli_passa_all_ao_adaptador(bruto, cli_com_banco):
    from typer.testing import CliRunner

    cli, conn, schema = cli_com_banco
    runner = CliRunner()
    ok = runner.invoke(cli.app, ["load", "sidra", "-s", str(bruto), "--all"])
    assert ok.exit_code == 0, ok.output
    assert conn.execute(f'SELECT count(*) FROM "{schema}".versao').fetchone() == (3,)
    falha = runner.invoke(cli.app, ["load", "sidra", "-s", str(bruto)])
    assert falha.exit_code == 1
    assert "--all" in falha.output


def test_cli_passa_historico_force_e_recriar(bruto, monkeypatch):
    from typer.testing import CliRunner

    from quantilica_sql import cli

    recebido = {}

    def falso(self, conn, source_path=None, **kwargs):
        recebido.update(kwargs)
        return 0

    class _Ctx:
        def __enter__(self):
            return object()

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(SidraAdapter, "load", falso)
    monkeypatch.setattr(cli, "get_connection", lambda **kw: _Ctx())
    res = CliRunner().invoke(
        cli.app,
        [
            "load",
            "sidra",
            "-s",
            str(bruto),
            "--all",
            "--historico",
            "--force",
            "--recriar",
        ],
    )
    assert res.exit_code == 0, res.output
    assert (recebido["historico"], recebido["force"], recebido["recriar"]) == (
        True,
        True,
        True,
    )
    assert recebido["all_partitions"] is True


def _cria_antigo(conn, schema):
    conn.execute(f'CREATE SCHEMA "{schema}"')
    conn.execute(
        f"""CREATE TABLE "{schema}".dados (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            tabela_sidra_id TEXT NOT NULL, localidade_id BIGINT NOT NULL,
            dimensao_id BIGINT NOT NULL, periodo_id INTEGER NOT NULL,
            modificacao DATE NOT NULL DEFAULT CURRENT_DATE,
            ativo BOOLEAN NOT NULL DEFAULT TRUE, v TEXT NOT NULL)"""
    )
    conn.execute(
        f'INSERT INTO "{schema}".dados (tabela_sidra_id, localidade_id, dimensao_id, '
        "periodo_id, v) VALUES ('1086', 1, 2, 3, '4.5')"
    )


def test_esquema_antigo_o_load_sai_com_1_e_a_mensagem_traz_o_remedio(
    bruto, cli_com_banco
):
    from typer.testing import CliRunner

    cli, conn, schema = cli_com_banco
    _cria_antigo(conn, schema)
    res = CliRunner().invoke(cli.app, ["load", "sidra", "-s", str(bruto), "--all"])
    assert res.exit_code == 1
    assert "--recriar" in res.output and "dados_legado_v0" in res.output
    assert conn.execute(f'SELECT count(*) FROM "{schema}".dados').fetchone() == (1,)


def test_esquema_antigo_com_recriar_renomeia_e_carrega(bruto, cli_com_banco):
    from typer.testing import CliRunner

    cli, conn, schema = cli_com_banco
    _cria_antigo(conn, schema)
    res = CliRunner().invoke(
        cli.app, ["load", "sidra", "-s", str(bruto), "--all", "--recriar"]
    )
    assert res.exit_code == 0, res.output
    assert conn.execute(
        f'SELECT count(*) FROM "{schema}".dados_legado_v0'
    ).fetchone() == (1,)
    assert conn.execute(f'SELECT count(*) FROM "{schema}".versao').fetchone() == (3,)
    # sem o esquema antigo, --recriar é inofensivo: a segunda carga pula tudo
    assert (
        CliRunner()
        .invoke(cli.app, ["load", "sidra", "-s", str(bruto), "--all", "--recriar"])
        .exit_code
        == 0
    )
    assert conn.execute(f'SELECT count(*) FROM "{schema}".versao').fetchone() == (3,)


def test_get_adapter_sidra_e_o_novo():
    assert isinstance(get_adapter("sidra"), SidraAdapter)
