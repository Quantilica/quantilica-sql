"""Valores do SIDRA: número exato ou símbolo, nunca zero inventado."""

from __future__ import annotations

import collections
import json
from decimal import Decimal

import pytest
from _sidra_fixtures import DADOS, unico

from quantilica_sql.sidra import SidraFonteError, converter_valor, ler_arquivo

ARQ_1612 = unico((DADOS / "t-1612").glob("t-1612_*@????????.json"))
ARQ_6381 = unico((DADOS / "t-6381").glob("t-6381_*@????????.json"))
ARQ_1086N = unico((DADOS / "ordens-de-url" / "t-1086").glob("*n1n3@????????.json"))


@pytest.mark.parametrize("simbolo", ["-", "..", "...", "X", "Z", ""])
def test_simbolos_vao_para_simbolo_e_o_valor_fica_nulo(simbolo):
    assert converter_valor(simbolo) == (None, simbolo)


@pytest.mark.parametrize(
    "texto", ["0", "0.0", "0.00", "-0.5", "7695.8200000000000", "12"]
)
def test_numero_vira_decimal_com_a_escala_do_texto(texto):
    valor, simbolo = converter_valor(texto)
    assert simbolo is None
    assert valor == Decimal(texto)
    assert str(valor) == texto


def test_zero_e_numero_e_nao_simbolo():
    for texto in ("0", "0.0"):
        valor, simbolo = converter_valor(texto)
        assert valor == 0 and simbolo is None


def test_menos_sozinho_e_simbolo_e_nao_zero():
    assert converter_valor("-") == (None, "-")
    assert converter_valor("-1") == (Decimal("-1"), None)


@pytest.mark.parametrize(
    "texto", ["abc", "1,5", "1e3", "NaN", "Infinity", "--", "...."]
)
def test_texto_desconhecido_para_a_carga(texto):
    with pytest.raises(SidraFonteError, match="não é número nem símbolo"):
        converter_valor(texto, origem="t-1.json (linha 7)")


def _contagem_crua(arquivo):
    linhas = json.loads(arquivo.read_text(encoding="utf-8"))[1:]
    simbolos = collections.Counter(
        r["V"] for r in linhas if r["V"] in {"-", "..", "...", "X", "Z", ""}
    )
    return linhas, simbolos


@pytest.mark.parametrize(
    "arquivo", [ARQ_1612, ARQ_6381, ARQ_1086N], ids=lambda a: a.name[:6]
)
def test_arquivo_real_contagem_de_simbolos_igual_a_do_cru(arquivo):
    linhas, esperado = _contagem_crua(arquivo)
    lido = ler_arquivo(arquivo)
    assert len(lido.celulas) == len(linhas)
    contados = collections.Counter(
        c.simbolo for c in lido.celulas if c.simbolo is not None
    )
    assert contados == esperado
    assert sum(esperado.values()) > 0
    # nunca valor e símbolo juntos, nem nenhum dos dois
    assert all((c.valor is None) != (c.simbolo is None) for c in lido.celulas)


def test_1612_tem_os_tres_simbolos_e_6381_o_z_e_1086_o_x():
    assert set(_contagem_crua(ARQ_1612)[1]) == {"-", "..", "..."}
    assert set(_contagem_crua(ARQ_6381)[1]) == {"Z"}
    assert "X" in _contagem_crua(ARQ_1086N)[1]


@pytest.mark.parametrize(
    "arquivo", [ARQ_1612, ARQ_6381, ARQ_1086N], ids=lambda a: a.name[:6]
)
def test_ida_e_volta_do_numero_e_identica_ao_texto(arquivo):
    linhas = json.loads(arquivo.read_text(encoding="utf-8"))[1:]
    lido = ler_arquivo(arquivo)
    numericos = [
        (r["V"], c.valor)
        for r, c in zip(linhas, lido.celulas, strict=True)
        if c.valor is not None
    ]
    assert numericos
    assert all(str(v) == texto for texto, v in numericos)


def test_decimal_com_escala_de_um_arquivo_real():
    arq = unico((DADOS / "t-1737").glob("t-1737_*@????????.json"))
    valores = {str(c.valor) for c in ler_arquivo(arq).celulas}
    assert "7706.5900000000000" in valores
    assert Decimal("7706.5900000000000").as_tuple().exponent == -13


def test_valor_inesperado_no_arquivo_para_a_leitura_com_erro_claro(tmp_path):
    import shutil

    pasta = tmp_path / "t-1737"
    shutil.copytree(DADOS / "t-1737", pasta)
    arq = unico(pasta.glob("t-1737_*@????????.json"))
    linhas = json.loads(arq.read_text(encoding="utf-8"))
    linhas[2]["V"] = "n/d"
    arq.write_text(json.dumps(linhas), encoding="utf-8")
    with pytest.raises(SidraFonteError, match=r"linha 3.*'n/d'"):
        ler_arquivo(arq)
