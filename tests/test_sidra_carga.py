"""Carga do SIDRA no PostgreSQL: ledger, versões, vigência por célula e as-of.

Exige ``QUANTILICA_TEST_DATABASE_URL`` (banco descartável); schema próprio por teste.
"""

from __future__ import annotations

import datetime as dt
import json
import logging

import pytest
from _sidra_fixtures import (
    DADOS,
    arquivo_de_dados,
    copia_tabelas,
    nova_versao,
    reescreve,
    remove_sidecar,
)

from quantilica_sql.sidra.carga import consulta_do_nome

TODAS = ("1086", "1612", "1737", "6381", "5932", "7060", "6579")


def _q(conn, schema, query, params=None):
    cur = conn.execute(query.replace("{s}", f'"{schema}"'), params)
    return cur.fetchall() if cur.description else []


def _um(conn, schema, query, params=None):
    return _q(conn, schema, query, params)[0][0]


def _n_linhas(arq):
    return len(json.loads(arq.read_text(encoding="utf-8"))) - 1


def _celulas(conn, schema):
    """``{(variavel, localidade, periodo): [(valor, simbolo, ativo, modificacao)]}``"""
    out: dict = {}
    for var, loc, per, valor, simb, ativo, mod in _q(
        conn,
        schema,
        "SELECT variavel_id, localidade_codigo, periodo_codigo, valor::text, simbolo, "
        "ativo, modificacao FROM {s}.observacao_plana ORDER BY modificacao NULLS FIRST",
    ):
        out.setdefault((var, loc, per), []).append((valor, simb, ativo, mod))
    return out


def _ativas(conn, schema):
    return {
        k: [x for x in v if x[2]]
        for k, v in _celulas(conn, schema).items()
        if any(x[2] for x in v)
    }


# --------------------------------------------------------------------------
# carga
# --------------------------------------------------------------------------
def test_carga_as_sete_fixtures_reais_carregam_sem_unique_violation(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", *TODAS)
    esperado = sum(_n_linhas(arquivo_de_dados(raiz, t)) for t in TODAS)
    assert adapter.load(conn, raiz, all_partitions=True) == esperado
    assert _um(conn, schema, "SELECT count(*) FROM {s}.observacao") == esperado
    for t in TODAS:
        n = _um(
            conn,
            schema,
            "SELECT count(*) FROM {s}.observacao WHERE tabela_id = %s",
            (int(t),),
        )
        assert n == _n_linhas(arquivo_de_dados(raiz, t)), t
    assert (
        _um(conn, schema, "SELECT count(*) FROM {s}.observacao WHERE ativo") == esperado
    )


def test_carga_1086_tem_cinco_dimensoes_e_as_series_com_duas_classificacoes(
    pg, tmp_path
):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1086")
    adapter.load(conn, raiz, all_partitions=True)
    cats = _q(
        conn,
        schema,
        "SELECT count(*), count(DISTINCT ordem), min(ordem), max(ordem) "
        "FROM {s}.serie_categoria",
    )[0]
    assert cats[1:] == (2, 0, 1)  # duas classificações por série
    dim = _q(conn, schema, "SELECT d4n, d5n, d6n FROM {s}.dimensao LIMIT 1")[0]
    assert dim[0] and dim[1] and dim[2] is None


def test_carga_simbolos_zero_e_valores_exatos(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1612", "6381", "1737")
    adapter.load(conn, raiz, all_partitions=True)
    simbolos = dict(
        _q(
            conn,
            schema,
            "SELECT simbolo, count(*) FROM {s}.observacao "
            "WHERE simbolo IS NOT NULL GROUP BY 1",
        )
    )
    cru = json.loads(arquivo_de_dados(raiz, "1612").read_text(encoding="utf-8"))[1:]
    cru += json.loads(arquivo_de_dados(raiz, "6381").read_text(encoding="utf-8"))[1:]
    for s in ("-", "..", "...", "Z"):
        assert simbolos[s] == sum(1 for r in cru if r["V"] == s)
    assert (
        _um(
            conn,
            schema,
            "SELECT count(*) FROM {s}.observacao WHERE valor = 0 "
            "AND simbolo IS NOT NULL",
        )
        == 0
    )
    assert (
        _um(
            conn,
            schema,
            "SELECT count(*) FROM {s}.dados WHERE v = '7706.5900000000000'",
        )
        == 1
    )


def test_carga_modificacao_vem_do_sidecar_do_carimbo_e_fica_nula_sem_data(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    real = arquivo_de_dados(raiz, "1737")  # sidecar com modificacao 2026-11-12
    sem_sidecar = nova_versao(real, "20261201", sufixo="_x")  # carimbo do nome
    sem_data = nova_versao(real, "00010101", sufixo="_y")  # a API não informou
    for a in (real, sem_sidecar, sem_data):
        adapter.load(conn, a)
    mods = dict(_q(conn, schema, "SELECT arquivo, modificacao FROM {s}.versao"))
    assert mods[real.name] == dt.date(2026, 11, 12)
    assert mods[sem_sidecar.name] == dt.date(2026, 12, 1)
    assert mods[sem_data.name] is None


def test_carga_sidecar_vale_mais_que_o_carimbo(pg, tmp_path):
    from quantilica.core.manifests import read_manifest_sidecar  # noqa: F401

    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    arq = arquivo_de_dados(raiz, "1737")
    sc = arq.with_name(arq.name + ".manifest.json")
    m = json.loads(sc.read_text(encoding="utf-8"))
    m["metadata"]["modificacao"] = "2026-11-10"
    sc.write_text(json.dumps(m), encoding="utf-8")
    adapter.load(conn, arq)
    assert _um(conn, schema, "SELECT modificacao FROM {s}.versao") == dt.date(
        2026, 11, 10
    )


def test_carga_sem_metadados_e_sem_sidecar_para_com_erro_claro_e_nada_fica(
    pg, tmp_path
):
    from quantilica_sql.sidra import SidraFonteError

    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    for p in (raiz / "t-1737").glob("*.manifest.json"):
        p.unlink()
    for p in (raiz / "t-1737").glob("metadados@*"):
        p.unlink()
    with pytest.raises(SidraFonteError, match="metadados"):
        adapter.load(conn, raiz, all_partitions=True)
    assert _um(conn, schema, "SELECT count(*) FROM {s}.observacao") == 0
    assert _um(conn, schema, "SELECT count(*) FROM {s}.arquivo_carregado") == 0


# --------------------------------------------------------------------------
# ledger e sha256
# --------------------------------------------------------------------------
def test_ledger_arquivo_igual_pula_sem_mexer_em_nada(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    assert adapter.load(conn, raiz, all_partitions=True) == 6
    antes = _q(
        conn,
        schema,
        "SELECT nome_arquivo, sha256, linhas, carregado_em FROM {s}.arquivo_carregado",
    )
    assert antes[0][1]  # sha256 gravado
    assert adapter.load(conn, raiz, all_partitions=True) == 0
    depois = _q(
        conn,
        schema,
        "SELECT nome_arquivo, sha256, linhas, carregado_em FROM {s}.arquivo_carregado",
    )
    assert antes == depois
    assert _um(conn, schema, "SELECT count(*) FROM {s}.versao") == 1


def test_ledger_hash_diferente_com_o_mesmo_nome_substitui_a_versao(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    arq = arquivo_de_dados(raiz, "1737")
    adapter.load(conn, arq)
    vid, sha = _q(
        conn,
        schema,
        "SELECT v.id, a.sha256 FROM {s}.versao v "
        "JOIN {s}.arquivo_carregado a ON a.nome_arquivo = v.arquivo",
    )[0]
    reescreve(arq, lambda r: {**r, "V": "9.99"} if r["D2C"] == "63" else r)
    assert adapter.load(conn, arq) == 6
    assert _um(conn, schema, "SELECT count(*) FROM {s}.observacao") == 6
    assert _um(conn, schema, "SELECT count(*) FROM {s}.versao") == 1
    assert _um(conn, schema, "SELECT id FROM {s}.versao") == vid  # mesma versão
    assert _um(conn, schema, "SELECT sha256 FROM {s}.arquivo_carregado") != sha
    assert (
        _um(
            conn,
            schema,
            "SELECT valor::text FROM {s}.observacao_plana WHERE variavel_id = 63",
        )
        == "9.99"
    )


def test_ledger_force_recarrega_as_mesmas_linhas(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    adapter.load(conn, raiz, all_partitions=True)
    assert adapter.load(conn, raiz, all_partitions=True) == 0
    assert adapter.load(conn, raiz, all_partitions=True, force=True) == 6
    assert _um(conn, schema, "SELECT count(*) FROM {s}.observacao") == 6
    assert _um(conn, schema, "SELECT count(*) FROM {s}.observacao WHERE ativo") == 6
    assert _um(conn, schema, "SELECT count(*) FROM {s}.versao") == 1


def test_ledger_registro_legado_so_ganha_o_hash(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    arq = arquivo_de_dados(raiz, "1737")
    adapter.load(conn, arq)
    _q(conn, schema, "UPDATE {s}.arquivo_carregado SET sha256 = NULL")
    assert adapter.load(conn, arq) == 0
    assert _um(conn, schema, "SELECT sha256 FROM {s}.arquivo_carregado") is not None
    assert _um(conn, schema, "SELECT count(*) FROM {s}.observacao") == 6


# --------------------------------------------------------------------------
# revisão do IBGE
# --------------------------------------------------------------------------
def _revisao(raiz):
    """v1 (real, 12/11) e v2 (01/12): um valor alterado e uma célula a menos."""
    v1 = arquivo_de_dados(raiz, "1737")
    v2 = nova_versao(
        v1,
        "20261201",
        lambda r: (
            None
            if r["D2C"] == "2266"
            else ({**r, "V": "0.15"} if r["D2C"] == "63" else r)
        ),
    )
    return v1, v2


def test_revisao_versao_nova_fica_ativa_e_a_antiga_vira_historia(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    v1, v2 = _revisao(raiz)
    adapter.load(conn, v1)
    assert adapter.load(conn, v2) == 5  # sem UniqueViolation
    ativas = _ativas(conn, schema)
    assert len(ativas) == 5
    assert all(x[0][3] == dt.date(2026, 12, 1) for x in ativas.values())
    assert ativas[(63, "1", "202610")][0][0] == "0.15"
    # a célula que a v2 deixou de trazer saiu da vigência e continua como história
    todas = _celulas(conn, schema)
    assert (2266, "1", "202610") not in ativas
    assert [x[2] for x in todas[(2266, "1", "202610")]] == [False]
    assert _um(conn, schema, "SELECT count(*) FROM {s}.observacao") == 11
    assert (
        _um(
            conn,
            schema,
            "SELECT count(*) FROM {s}.observacao o JOIN {s}.versao v ON v.id = o.versao_id "
            "WHERE v.arquivo = %s AND o.ativo",
            (v1.name,),
        )
        == 0
    )


def test_revisao_recarregar_o_conjunto_nao_muda_nada(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    _revisao(raiz)
    assert adapter.load(conn, raiz, all_partitions=True, historico=True) == 11
    foto = _q(conn, schema, "SELECT * FROM {s}.observacao ORDER BY 1,3,4,5,8")
    assert adapter.load(conn, raiz, all_partitions=True, historico=True) == 0
    assert _q(conn, schema, "SELECT * FROM {s}.observacao ORDER BY 1,3,4,5,8") == foto


def test_revisao_all_carrega_so_a_vigente_e_historico_todas(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    v1, v2 = _revisao(raiz)
    assert adapter.load(conn, raiz, all_partitions=True) == 5
    assert [r[0] for r in _q(conn, schema, "SELECT arquivo FROM {s}.versao")] == [
        v2.name
    ]
    assert adapter.load(conn, raiz, all_partitions=True, historico=True) == 6  # + v1
    assert _um(conn, schema, "SELECT count(*) FROM {s}.versao") == 2
    # a v1 entrou depois, mas mais velha: só história, a v2 continua valendo
    assert _um(conn, schema, "SELECT count(*) FROM {s}.observacao WHERE ativo") == 5
    assert _ativas(conn, schema)[(63, "1", "202610")][0][0] == "0.15"


def test_revisao_historico_carrega_em_ordem_de_data_independente_do_nome(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    _revisao(raiz)
    adapter.load(conn, raiz, all_partitions=True, historico=True)
    ordem = [
        r[0] for r in _q(conn, schema, "SELECT modificacao FROM {s}.versao ORDER BY id")
    ]
    assert ordem == [dt.date(2026, 11, 12), dt.date(2026, 12, 1)]


# --------------------------------------------------------------------------
# as-of
# --------------------------------------------------------------------------
def test_asof_dados_em_devolve_a_versao_que_valia_na_data(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    _revisao(raiz)
    adapter.load(conn, raiz, all_partitions=True, historico=True)

    def valor(data, var=63):
        return _q(
            conn,
            schema,
            "SELECT v FROM {s}.dados_em(%s) WHERE dimensao_id = "
            "(SELECT id FROM {s}.serie WHERE variavel_id = %s)",
            (data, var),
        )

    assert valor("2026-11-20") == [("0.14",)]  # antes da revisão: o valor antigo
    assert valor("2026-12-01") == [("0.15",)]  # a partir da data: o novo
    assert valor("2027-01-01") == [("0.15",)]
    assert valor("2026-11-11") == []  # antes de qualquer versão
    # a célula que a v2 deixou de trazer ainda existia em 20/11
    assert valor("2026-11-20", 2266) == [("7706.5900000000000",)]


def test_asof_a_receita_antiga_do_legado_perde_a_chave_revisada(pg, tmp_path):
    """``modificacao <= D AND ativo`` descarta o que foi revisado depois de D."""
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    _revisao(raiz)
    adapter.load(conn, raiz, all_partitions=True, historico=True)
    errada = _q(
        conn,
        schema,
        "SELECT v FROM {s}.dados WHERE tabela_sidra_id = '1737' "
        "AND modificacao <= '2026-11-20' AND ativo",
    )
    certa = _q(
        conn,
        schema,
        "SELECT v FROM {s}.dados_em('2026-11-20') WHERE tabela_sidra_id = '1737'",
    )
    assert errada == []  # a v1 está inativa (revisada) e a v2 é posterior a D
    assert len(certa) == 6  # a consulta certa devolve as 6 células de 20/11


def test_asof_versao_sem_data_nao_entra(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    real = arquivo_de_dados(raiz, "1737")
    sem_data = nova_versao(real, "00010101", sufixo="_sd")
    adapter.load(conn, sem_data)
    assert _um(conn, schema, "SELECT count(*) FROM {s}.observacao") == 6
    assert _q(conn, schema, "SELECT * FROM {s}.dados_em('2999-01-01')") == []


# --------------------------------------------------------------------------
# sobreposição de pedaços e empate
# --------------------------------------------------------------------------
def _pedacos(raiz):
    """A (6 variáveis, 12/11) e B (só a variável 63, outro nome de consulta)."""
    a = arquivo_de_dados(raiz, "1737")
    return a, lambda stamp, v: nova_versao(
        a, stamp, lambda r: {**r, "V": v} if r["D2C"] == "63" else None, sufixo="_so63"
    )


def test_sobreposicao_a_de_maior_data_vence_qualquer_que_seja_o_nome(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a, faz_b = _pedacos(raiz)
    adapter.load(conn, a)
    adapter.load(conn, faz_b("20261201", "0.77"))
    ativas = _ativas(conn, schema)
    assert len(ativas) == 6  # sem célula duplicada
    assert ativas[(63, "1", "202610")][0][:2] == ("0.77", None)


def test_sobreposicao_a_mais_antiga_que_chega_depois_fica_so_como_historia(
    pg, tmp_path
):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a, faz_b = _pedacos(raiz)
    adapter.load(conn, a)
    adapter.load(conn, faz_b("20261001", "0.01"))
    ativas = _ativas(conn, schema)
    assert ativas[(63, "1", "202610")][0][0] == "0.14"
    assert len(_celulas(conn, schema)[(63, "1", "202610")]) == 2


def test_sobreposicao_empate_de_data_mantem_a_ativa_e_avisa_com_a_contagem(
    pg, tmp_path, caplog
):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a, faz_b = _pedacos(raiz)
    adapter.load(conn, a)
    with caplog.at_level(logging.WARNING, logger="quantilica_sql.sidra.carga"):
        adapter.load(conn, faz_b("20261112", "0.99"))  # mesma data, outro valor
    assert _ativas(conn, schema)[(63, "1", "202610")][0][0] == "0.14"  # a anterior
    avisos = [r.getMessage() for r in caplog.records if "mesma data" in r.getMessage()]
    assert (
        len(avisos) == 1
        and avisos[0].startswith("SIDRA t-1737")
        and " 1 célula" in avisos[0]
    )
    meta = _um(
        conn,
        schema,
        "SELECT metadados->>'empates_divergentes' FROM {s}.arquivo_carregado "
        "WHERE nome_arquivo LIKE '%so63%'",
    )
    assert meta == "1"


def test_sobreposicao_empate_com_o_mesmo_valor_nao_avisa(pg, tmp_path, caplog):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a, faz_b = _pedacos(raiz)
    adapter.load(conn, a)
    with caplog.at_level(logging.WARNING, logger="quantilica_sql.sidra.carga"):
        adapter.load(conn, faz_b("20261112", "0.14"))
    assert not [r for r in caplog.records if "mesma data" in r.getMessage()]
    assert len(_ativas(conn, schema)) == 6


def test_sobreposicao_as_quatro_ordens_de_url_da_1086_nao_duplicam_celulas(
    pg, tmp_path
):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1086")
    base = arquivo_de_dados(raiz, "1086")
    variantes = sorted(
        (DADOS / "ordens-de-url" / "t-1086").glob("t-1086_*_o-*@????????.json")
    )
    assert len(variantes) == 4
    import shutil

    for v in variantes:
        shutil.copy(v, raiz / "t-1086" / v.name)
        shutil.copy(
            v.with_name(v.name + ".manifest.json"),
            raiz / "t-1086" / (v.name + ".manifest.json"),
        )
    adapter.load(conn, raiz, all_partitions=True)
    assert _um(conn, schema, "SELECT count(*) FROM {s}.versao") == 5
    distintas = _um(
        conn,
        schema,
        "SELECT count(*) FROM (SELECT DISTINCT serie_id, "
        "localidade_id, periodo_id FROM {s}.observacao) x",
    )
    assert (
        _um(conn, schema, "SELECT count(*) FROM {s}.observacao WHERE ativo")
        == distintas
    )
    assert _n_linhas(base) <= distintas


# --------------------------------------------------------------------------
# reconciliação ao substituir uma versão
# --------------------------------------------------------------------------
def test_reconcilia_celula_volta_pela_consulta_mais_antiga_e_a_unica_sai(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a = arquivo_de_dados(raiz, "1737")  # consulta A, 12/11, as 6 variáveis
    # consulta B, 01/12, cobre as variáveis 63 e 69 com outros valores
    b = nova_versao(
        a,
        "20261201",
        lambda r: {**r, "V": "5.55"} if r["D2C"] in ("63", "69") else None,
        sufixo="_b",
    )
    adapter.load(conn, a)
    adapter.load(conn, b)
    ativas = _ativas(conn, schema)
    assert ativas[(63, "1", "202610")][0][0] == "5.55"
    assert ativas[(69, "1", "202610")][0][0] == "5.55"
    # B é substituída (mesmo nome, outro hash) por B', que cobre só a 63; e traz
    # uma célula nova (variável 9999) que só ela cobre
    reescreve(
        b,
        lambda r: (
            {**r, "V": "6.66"}
            if r["D2C"] == "63"
            else (
                {**r, "D2C": "9999", "D2N": "Variável nova"}
                if r["D2C"] == "69"
                else None
            )
        ),
    )
    adapter.load(conn, b)
    ativas = _ativas(conn, schema)
    assert ativas[(63, "1", "202610")][0][0] == "6.66"
    # a 69 que B' deixou de cobrir volta a valer pela consulta A (a mais antiga)
    assert ativas[(69, "1", "202610")][0][0] != "5.55"
    assert ativas[(69, "1", "202610")][0][3] == dt.date(2026, 11, 12)
    # a 9999 só existe na B'
    assert ativas[(9999, "1", "202610")][0][0] == "5.55"
    # uma célula que só a B cobria e a B' deixou de cobrir sai da vigência
    assert len(ativas) == 7


def test_reconcilia_celula_que_so_a_versao_substituida_cobria_sai_da_vigencia(
    pg, tmp_path
):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a = arquivo_de_dados(raiz, "1737")
    b = nova_versao(
        a,
        "20261201",
        lambda r: {**r, "D3C": "202609", "D3N": "setembro 2026"},
        sufixo="_b",
    )
    adapter.load(conn, a)
    adapter.load(conn, b)  # o período 202609 só a B cobre
    assert len([k for k in _ativas(conn, schema) if k[2] == "202609"]) == 6
    reescreve(b, lambda r: r if r["D2C"] == "63" else None)  # B' perde 5 células
    adapter.load(conn, b)
    ativas = _ativas(conn, schema)
    assert len([k for k in ativas if k[2] == "202609"]) == 1
    assert len([k for k in ativas if k[2] == "202610"]) == 6  # as da A seguem


# --------------------------------------------------------------------------
# atomicidade
# --------------------------------------------------------------------------
def _vazio(conn, schema):
    for t in (
        "observacao",
        "versao",
        "tabela",
        "serie",
        "localidade",
        "periodo",
        "arquivo_carregado",
        "variavel",
        "categoria",
    ):
        assert _um(conn, schema, f"SELECT count(*) FROM {{s}}.{t}") == 0, t


def test_atomico_simbolo_inesperado_nao_deixa_nada(pg, tmp_path):
    from quantilica_sql.sidra import SidraFonteError

    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    arq = arquivo_de_dados(raiz, "1737")
    reescreve(arq, lambda r: {**r, "V": "n/d"} if r["D2C"] == "69" else r)
    with pytest.raises(SidraFonteError, match="n/d"):
        adapter.load(conn, arq)
    _vazio(conn, schema)


def test_atomico_falha_no_meio_da_carga_desfaz_tudo_e_o_cache_nao_sobrevive(
    pg, tmp_path, monkeypatch
):
    from quantilica_sql.sidra.carga import Carregador

    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737", "6381")
    original = Carregador._vigencia

    def falha(self, *a, **k):
        raise RuntimeError("falha injetada depois do COPY")

    monkeypatch.setattr(Carregador, "_vigencia", falha)
    with pytest.raises(RuntimeError, match="injetada"):
        adapter.load(conn, raiz, all_partitions=True)
    _vazio(conn, schema)
    # sem a falha, a mesma carga entra inteira (ids em cache de uma transação
    # desfeita não podem sobreviver)
    monkeypatch.setattr(Carregador, "_vigencia", original)
    assert adapter.load(conn, raiz, all_partitions=True) == 12
    assert _um(conn, schema, "SELECT count(*) FROM {s}.observacao") == 12


def test_atomico_arquivo_com_falha_nao_impede_o_anterior_ja_confirmado(pg, tmp_path):
    from quantilica_sql.sidra import SidraFonteError

    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737", "6381")
    ruim = arquivo_de_dados(raiz, "6381")
    reescreve(ruim, lambda r: {**r, "V": "??"})
    with pytest.raises(SidraFonteError):
        adapter.load(conn, raiz, all_partitions=True)  # t-1737 entra, t-6381 falha
    assert (
        _um(conn, schema, "SELECT count(DISTINCT tabela_id) FROM {s}.observacao") == 1
    )
    assert _um(conn, schema, "SELECT count(*) FROM {s}.arquivo_carregado") == 1


# --------------------------------------------------------------------------
# view plana com as fixtures
# --------------------------------------------------------------------------
def test_plana_tem_uma_linha_de_cada_fixture_com_categorias_em_jsonb(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", *TODAS)
    adapter.load(conn, raiz, all_partitions=True)
    por_tabela = dict(
        _q(
            conn,
            schema,
            "SELECT tabela_id, count(*) FROM {s}.observacao_plana GROUP BY 1",
        )
    )
    assert set(por_tabela) == {int(t) for t in TODAS}
    cats = _um(
        conn,
        schema,
        "SELECT categorias FROM {s}.observacao_plana WHERE tabela_id = 1086 LIMIT 1",
    )
    assert set(cats) == {"Referência temporal", "Tipo de inspeção"}
    assert (
        _um(
            conn,
            schema,
            "SELECT categorias FROM {s}.observacao_plana "
            "WHERE tabela_id = 1737 LIMIT 1",
        )
        == {}
    )
    n1 = _q(
        conn,
        schema,
        "SELECT nivel, localidade, periodo, unidade FROM {s}.observacao_plana "
        "WHERE tabela_id = 1737 AND variavel_id = 63",
    )
    assert n1 == [("Brasil", "Brasil", "outubro 2026", "%")]


def test_consulta_do_nome():
    assert consulta_do_nome("t-1_p-2_f-a_n1-all_v-all@20261112.json") == (
        "t-1_p-2_f-a_n1-all_v-all"
    )
    assert consulta_do_nome("series_433@20261120T101010.json") == "series_433"
    assert consulta_do_nome("sem_carimbo.json") == "sem_carimbo"


def test_remove_sidecar_helper_nao_falha_sem_sidecar(tmp_path):
    arq = tmp_path / "x.json"
    arq.write_text("[]")
    remove_sidecar(arq)
