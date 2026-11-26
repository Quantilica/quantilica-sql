"""Papel de cada dimensão D<i> do SIDRA: pela URL do sidecar e pelo cabeçalho."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from _sidra_fixtures import CASOS, DADOS, C, P, T, V

from quantilica_sql.sidra import (
    SidraFonteError,
    ler_arquivo,
    ler_metadados,
    metadados_da_pasta,
    papeis_por_cabecalho,
    papeis_por_url,
    resolver_papeis,
)


def test_ha_catorze_casos():
    assert len(CASOS) == 14


def _sidecar_url(arquivo: Path) -> str:
    return json.loads(
        arquivo.with_name(arquivo.name + ".manifest.json").read_text(encoding="utf-8")
    )["url"]


@pytest.mark.parametrize("nome", list(CASOS))
def test_papeis_por_url_do_caso(nome):
    arquivo, esperado = CASOS[nome]
    assert papeis_por_url(_sidecar_url(arquivo)) == esperado


def test_papeis_por_url_varios_n_contam_como_um_territorio():
    url = "https://apisidra.ibge.gov.br/values/t/1/n1/all/n3/11,12/v/all/p/2020/h/y/f/a/d/m"
    assert papeis_por_url(url) == [T, V, P]


def test_papeis_por_url_n_separados_ficam_na_posicao_do_primeiro():
    url = "https://x/values/t/1/n1/all/p/2020/n3/all/v/all/c1/all"
    assert papeis_por_url(url) == [T, P, V, C(1)]


def test_papeis_por_url_ordem_das_classificacoes_e_a_da_url():
    url = (
        "https://x/values/t/1/c20/all/c10/1,2/n6/all/v/63/p/last%201/h/y/f/a/d/v63%202"
    )
    assert papeis_por_url(url) == [C(20), C(10), T, V, P]


def test_papeis_por_url_recusa_url_sem_variavel_ou_segmento_estranho():
    with pytest.raises(SidraFonteError, match="0 dimensões de variavel"):
        papeis_por_url("https://x/values/t/1/n1/all/p/2020")
    with pytest.raises(SidraFonteError, match="desconhecido"):
        papeis_por_url("https://x/values/t/1/n1/all/v/all/p/1/z/9")
    with pytest.raises(SidraFonteError, match="sem valor"):
        papeis_por_url("https://x/values/t/1/n1/all/v")


@pytest.mark.parametrize("nome", list(CASOS))
def test_papeis_por_cabecalho_do_caso(nome):
    arquivo, esperado = CASOS[nome]
    linhas = json.loads(arquivo.read_text(encoding="utf-8"))
    meta = metadados_da_pasta(arquivo.parent)
    assert meta is not None
    assert papeis_por_cabecalho(linhas, meta) == esperado


@pytest.mark.parametrize("nome", list(CASOS))
def test_sem_sidecar_o_cabecalho_com_os_metadados_chega_ao_mesmo_papel(nome):
    arquivo, esperado = CASOS[nome]
    linhas = json.loads(arquivo.read_text(encoding="utf-8"))
    meta = metadados_da_pasta(arquivo.parent)
    sem_sidecar = arquivo.with_name("inexistente.json")
    assert resolver_papeis(sem_sidecar, linhas, meta) == esperado


@pytest.mark.parametrize("nome", list(CASOS))
def test_resolver_papeis_do_caso(nome):
    arquivo, esperado = CASOS[nome]
    linhas = json.loads(arquivo.read_text(encoding="utf-8"))
    assert resolver_papeis(arquivo, linhas) == esperado
    assert ler_arquivo(arquivo).papeis == esperado


# --------------------------------------------------------------------------
# erros
# --------------------------------------------------------------------------
def _copia_1086(tmp_path: Path) -> Path:
    origem = DADOS / "t-1086"
    destino = tmp_path / "t-1086"
    shutil.copytree(origem, destino)
    return destino / next(p.name for p in origem.glob("t-1086_*@*.json"))


def test_erro_divergencia_entre_url_e_cabecalho(tmp_path):
    arquivo = _copia_1086(tmp_path)
    sidecar = arquivo.with_name(arquivo.name + ".manifest.json")
    m = json.loads(sidecar.read_text(encoding="utf-8"))
    # a URL diz outra ordem do que o arquivo realmente tem
    m["url"] = m["url"].replace("/v/all/p/202601", "/p/202601/v/all")
    sidecar.write_text(json.dumps(m), encoding="utf-8")
    with pytest.raises(SidraFonteError) as exc:
        ler_arquivo(arquivo)
    msg = str(exc.value)
    assert arquivo.name in msg and "divergem" in msg


def test_erro_papel_ambiguo_duas_dimensoes_de_variavel(tmp_path):
    arquivo = _copia_1086(tmp_path)
    linhas = json.loads(arquivo.read_text(encoding="utf-8"))
    linhas[0]["D4N"] = "Variável"  # uma classificação com o nome da variável
    meta = metadados_da_pasta(arquivo.parent)
    with pytest.raises(SidraFonteError, match="papel ambíguo"):
        papeis_por_cabecalho(linhas, meta, origem=arquivo.name)


def test_erro_papel_ambiguo_duas_classificacoes_com_o_mesmo_nome(tmp_path):
    arquivo = _copia_1086(tmp_path)
    meta_arq = next(arquivo.parent.glob("metadados@????????.json"))
    bruto = json.loads(meta_arq.read_text(encoding="utf-8"))
    bruto["classificacoes"][1]["nome"] = bruto["classificacoes"][0]["nome"]
    meta_arq.write_text(json.dumps(bruto), encoding="utf-8")
    with pytest.raises(SidraFonteError, match="papel ambíguo"):
        ler_arquivo(arquivo)


def test_erro_dimensao_sem_papel(tmp_path):
    arquivo = _copia_1086(tmp_path)
    linhas = json.loads(arquivo.read_text(encoding="utf-8"))
    linhas[0]["D4N"] = "Dimensão que ninguém conhece"
    with pytest.raises(SidraFonteError, match="sem papel identificável"):
        papeis_por_cabecalho(linhas, metadados_da_pasta(arquivo.parent), origem="x")


def test_erro_sem_sidecar_e_sem_metadados(tmp_path):
    arquivo = _copia_1086(tmp_path)
    for p in arquivo.parent.glob("*.manifest.json"):
        p.unlink()
    for p in arquivo.parent.glob("metadados@*"):
        p.unlink()
    with pytest.raises(SidraFonteError) as exc:
        ler_arquivo(arquivo)
    msg = str(exc.value)
    assert arquivo.name in msg
    assert "sem sidecar" in msg and "metadados" in msg


def test_erro_sem_metadados_com_sidecar(tmp_path):
    arquivo = _copia_1086(tmp_path)
    for p in arquivo.parent.glob("metadados@*"):
        p.unlink()
    with pytest.raises(SidraFonteError) as exc:
        ler_arquivo(arquivo)
    msg = str(exc.value)
    assert arquivo.name in msg and "sem metadados@" in msg


def test_erro_classificacao_da_url_fora_dos_metadados(tmp_path):
    arquivo = _copia_1086(tmp_path)
    meta_arq = next(arquivo.parent.glob("metadados@????????.json"))
    bruto = json.loads(meta_arq.read_text(encoding="utf-8"))
    bruto["classificacoes"][0]["id"] = 99999
    bruto["classificacoes"][0]["nome"] = "Outra"
    meta_arq.write_text(json.dumps(bruto), encoding="utf-8")
    with pytest.raises(SidraFonteError):
        ler_arquivo(arquivo)


def test_erro_arquivo_sem_cabecalho_do_sidra(tmp_path):
    arquivo = _copia_1086(tmp_path)
    linhas = json.loads(arquivo.read_text(encoding="utf-8"))
    arquivo.write_text(json.dumps(linhas[1:]), encoding="utf-8")
    with pytest.raises(SidraFonteError, match="cabeçalho"):
        ler_arquivo(arquivo)


def test_erro_territorio_que_nao_confere_com_nc_nn(tmp_path):
    arquivo = _copia_1086(tmp_path)
    linhas = json.loads(arquivo.read_text(encoding="utf-8"))
    for r in linhas[1:]:
        r["NN"] = "Município"
        r["NC"] = "6"
    meta = metadados_da_pasta(arquivo.parent)
    with pytest.raises(SidraFonteError, match="NC/NN"):
        papeis_por_cabecalho(linhas, meta, origem="x")


def test_metadados_vigente_na_pasta_do_arquivo():
    meta = ler_metadados(next((DADOS / "t-1086").glob("metadados@*.json")))
    assert meta.tabela_id == 1086
