"""Metadados vigentes do SIDRA: variáveis, classificações, categorias e períodos."""

from __future__ import annotations

import datetime as dt
import shutil

import pytest
from _sidra_fixtures import DADOS

from quantilica_sql.sidra import (
    SidraFonteError,
    ler_metadados,
    metadados_da_pasta,
    metadados_vigente,
)


def test_le_variaveis_classificacoes_e_categorias_com_ordem():
    meta = metadados_da_pasta(DADOS / "t-1086")
    assert meta is not None
    assert meta.tabela_id == 1086
    assert meta.periodicidade == "trimestral"
    assert [v.id for v in meta.variaveis][:3] == [151, 1000151, 282]
    assert meta.variaveis[0].unidade == "Unidades"
    assert [(c.id, c.ordem) for c in meta.classificacoes] == [(12716, 0), (12529, 1)]
    assert meta.classificacoes[0].nome == "Referência temporal"
    assert {x.id for x in meta.classificacoes[0].categorias} >= {115236}
    assert meta.classificacao(12529) is meta.classificacoes[1]
    assert meta.classificacao(1) is None


def test_periodo_trimestral_traz_trimestre_e_datas():
    meta = metadados_da_pasta(DADOS / "t-1086")
    p = meta.periodos["202601"]
    assert (p.frequencia, p.ano, p.trimestre, p.mes, p.semestre) == (
        "trimestral",
        2026,
        1,
        None,
        None,
    )
    assert (p.data_inicio, p.data_fim) == (dt.date(2026, 1, 1), dt.date(2026, 3, 31))
    assert p.modificacao == dt.date(2026, 9, 15)
    assert p.rotulo == "1º trimestre 2026"


def test_periodo_anual_e_municipal():
    meta = metadados_da_pasta(DADOS / "t-6579")
    p = meta.periodos["2026"]
    assert (p.frequencia, p.ano, p.mes) == ("anual", 2026, None)
    assert meta.localidades[0].nivel_codigo == "6"  # "N6" da API vira o NC dos dados
    assert meta.localidades[0].nivel_nome == "Município"


def test_codigo_202609_mensal_e_trimestre_movel_ficam_distintos():
    mensal = metadados_da_pasta(DADOS / "t-1737").periodos["202609"]
    movel = metadados_da_pasta(DADOS / "t-6381").periodos["202609"]
    assert (mensal.frequencia, movel.frequencia) == ("mensal", "trimestre_movel")
    assert (mensal.data_inicio, mensal.data_fim) == (
        dt.date(2026, 9, 1),
        dt.date(2026, 9, 30),
    )
    assert (movel.data_inicio, movel.data_fim) == (
        dt.date(2026, 7, 1),
        dt.date(2026, 9, 30),
    )
    assert mensal.mes == movel.mes == 9
    assert mensal.rotulo != movel.rotulo


def test_vigente_e_o_metadados_mais_novo_da_pasta(tmp_path):
    pasta = tmp_path / "t-1737"
    shutil.copytree(DADOS / "t-1737", pasta)
    atual = next(pasta.glob("metadados@????????.json"))
    antigo = pasta / "metadados@20260101.json"
    shutil.copy(atual, antigo)
    novo = pasta / "metadados@20270101.json"
    shutil.copy(atual, novo)
    assert metadados_vigente(pasta) == novo
    novo.unlink()
    assert metadados_vigente(pasta) == atual  # 20261125 > 20260101
    assert ler_metadados(atual).arquivo == atual


def test_pasta_sem_metadados_devolve_none(tmp_path):
    assert metadados_vigente(tmp_path) is None
    assert metadados_da_pasta(tmp_path) is None


def test_metadados_fora_do_formato_da_erro_claro(tmp_path):
    arq = tmp_path / "metadados@20261125.json"
    arq.write_text('{"id": 1}', encoding="utf-8")
    with pytest.raises(SidraFonteError, match="fora do formato"):
        ler_metadados(arq)
    arq.write_text("{", encoding="utf-8")
    with pytest.raises(SidraFonteError, match="ilegíveis"):
        ler_metadados(arq)
