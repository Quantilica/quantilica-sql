"""Vigência por célula (revisão adversarial): o estado ativo é função das últimas
versões de cada consulta, nunca da ordem de carga, e ``dados_em`` o espelha.

Exige ``QUANTILICA_TEST_DATABASE_URL`` (banco descartável).
"""

from __future__ import annotations

import itertools
import json
import uuid

import pytest
from _sidra_fixtures import (
    arquivo_de_dados,
    copia_tabelas,
    nova_versao,
    reescreve,
)

from quantilica_sql.sidra import SidraFonteError
from quantilica_sql.sidra.carga import _SQL_CANDIDATAS, QUEDA_MAXIMA
from quantilica_sql.sidra.esquema import (
    ESQUEMA_VERSAO,
    EsquemaAntigoError,
    criar_esquema,
)


def _q(conn, schema, query, params=None):
    cur = conn.execute(query.replace("{s}", f'"{schema}"'), params)
    return cur.fetchall() if cur.description else []


def _ativas(conn, schema):
    return {
        (v, p): val
        for v, p, val in _q(
            conn,
            schema,
            "SELECT variavel_id, periodo_codigo, COALESCE(valor::text, simbolo) "
            "FROM {s}.observacao_plana WHERE ativo",
        )
    }


def _em(conn, schema, data, tabela=None):
    args = (data,) if tabela is None else (data, tabela)
    marcas = "%s" if tabela is None else "%s, %s"
    return {
        (v, p): val
        for v, p, val in _q(
            conn,
            schema,
            "SELECT s.variavel_id, pe.codigo, d.v "
            f"FROM {{s}}.dados_em({marcas}) d "
            "JOIN {s}.serie s ON s.id = d.dimensao_id "
            "JOIN {s}.periodo pe ON pe.id = d.periodo_id",
            args,
        )
    }


def _um_ativo_por_celula(conn, schema):
    return _q(
        conn,
        schema,
        "SELECT count(*) FROM (SELECT 1 FROM {s}.observacao WHERE ativo "
        "GROUP BY serie_id, localidade_id, periodo_id HAVING count(*) > 1) x",
    )[0][0]


def _abc(raiz):
    a = arquivo_de_dados(raiz, "1737")  # consulta A, 12/11, 6 variáveis
    b1 = nova_versao(
        a,
        "20261201",
        lambda r: {**r, "V": "5.55"} if r["D2C"] in ("63", "69") else None,
        sufixo="_b",
    )
    b2 = nova_versao(
        a,
        "20261215",
        lambda r: {**r, "V": "6.66"} if r["D2C"] == "63" else None,
        sufixo="_b",
    )
    return a, b1, b2


# --------------------------------------------------------------------------
# B1: dados_em espelha o ativo
# --------------------------------------------------------------------------
def test_asof_hoje_nao_devolve_celula_que_a_revisao_removeu(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a = arquivo_de_dados(raiz, "1737")
    v2 = nova_versao(a, "20261201", lambda r: None if r["D2C"] == "2266" else r)
    adapter.load(conn, a)
    adapter.load(conn, v2)
    ativas = _ativas(conn, schema)
    assert (2266, "202610") not in ativas
    assert _em(conn, schema, "2027-01-01") == ativas
    # e antes da revisão a célula ainda existia
    assert (2266, "202610") in _em(conn, schema, "2026-11-20")


def test_asof_empate_de_data_desempata_como_o_ativo_pela_primeira_carregada(
    pg, tmp_path
):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a = arquivo_de_dados(raiz, "1737")
    stamp = a.name.split("@")[1][:8]
    b = nova_versao(
        a,
        stamp,
        lambda r: {**r, "V": "9.99"} if r["D2C"] == "63" else None,
        sufixo="_b",
    )
    adapter.load(conn, a)
    adapter.load(conn, b)
    ativas = _ativas(conn, schema)
    assert ativas[(63, "202610")] == "0.14"  # a primeira carregada
    assert _em(conn, schema, "2027-01-01") == ativas


def test_asof_com_a_tabela_le_so_as_versoes_dela_e_usa_o_indice(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737", "7060")
    adapter.load(conn, raiz, all_partitions=True)
    so_1737 = _em(conn, schema, "2027-01-01", "1737")
    assert (
        so_1737
        == {
            k: v
            for k, v in _em(conn, schema, "2027-01-01").items()
            if k[0] in {2266, 63, 2263, 2264, 69, 2265}
        }
        or len(so_1737) == 6
    )
    assert _q(
        conn,
        schema,
        "SELECT DISTINCT tabela_sidra_id FROM {s}.dados_em('2027-01-01', '1737')",
    ) == [("1737",)]
    # o corpo da função, com a data e a tabela dadas, entra pelo índice da versão e
    # não varre o fato inteiro
    corpo = _q(
        conn,
        schema,
        "SELECT prosrc FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = %s AND proname = 'dados_em' AND pronargs = 2",
        (schema,),
    )[0][0]
    corpo = corpo.replace("<= data", "<= DATE '2027-01-01'").replace(
        "tabela::integer", "1737"
    )
    conn.execute("SET enable_seqscan = off")
    plano = "\n".join(
        r[0] for r in conn.execute("EXPLAIN " + corpo.replace("{s}", f'"{schema}"'))
    )
    assert "Seq Scan on observacao" not in plano
    assert "observacao_versao_idx" in plano


# --------------------------------------------------------------------------
# I1 e I2: o estado ativo não depende da ordem
# --------------------------------------------------------------------------
def test_ordem_de_carga_nao_muda_o_estado_ativo_nas_seis_permutacoes(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a, b1, b2 = _abc(raiz)
    estados = {}
    for ordem in itertools.permutations((a, b1, b2)):
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        for f in ordem:
            adapter.load(conn, f)
        ativas = _ativas(conn, schema)
        assert _um_ativo_por_celula(conn, schema) == 0
        assert ativas == _em(conn, schema, "2999-01-01"), [f.name for f in ordem]
        estados[tuple(f.name for f in ordem)] = ativas
    unicos = {json.dumps(sorted(map(str, e.items()))) for e in estados.values()}
    assert len(unicos) == 1  # as 6 ordens dão o mesmo conjunto ativo
    estado = next(iter(estados.values()))
    assert estado[(63, "202610")] == "6.66"  # B2, a última versão da consulta B
    assert estado[(69, "202610")] not in ("5.55", "6.66")  # B2 não a traz: vale a A
    assert len(estado) == 6


def test_all_e_historico_chegam_ao_mesmo_estado_ativo(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    _abc(raiz)
    adapter.load(conn, raiz, all_partitions=True)
    so_vigentes = _ativas(conn, schema)
    conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
    adapter.load(conn, raiz, all_partitions=True, historico=True)
    assert _ativas(conn, schema) == so_vigentes
    assert _um_ativo_por_celula(conn, schema) == 0
    assert so_vigentes == _em(conn, schema, "2999-01-01")


def test_revisao_que_deixa_de_trazer_a_celula_devolve_a_outra_consulta(pg, tmp_path):
    """B2 não traz a 69: a 69 passa a valer pela consulta A, mesmo com a B1 carregada."""
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a, b1, b2 = _abc(raiz)
    for f in (a, b1):
        adapter.load(conn, f)
    assert _ativas(conn, schema)[(69, "202610")] == "5.55"
    adapter.load(conn, b2)
    ativas = _ativas(conn, schema)
    assert ativas[(69, "202610")] != "5.55" and (69, "202610") in ativas
    assert ativas[(63, "202610")] == "6.66"


def test_reconcilia_nao_ressuscita_valor_de_versao_superada_de_outra_consulta(
    pg, tmp_path
):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a, b1, b2 = _abc(raiz)
    a.unlink()
    c = nova_versao(
        b1,
        "20261220",
        lambda r: {**r, "V": "7.77"} if r["D2C"] == "69" else None,
        sufixo="_c",
    )
    for f in (b1, b2, c):
        adapter.load(conn, f)
    assert _ativas(conn, schema)[(69, "202610")] == "7.77"
    reescreve(c, lambda r: {**r, "D2C": "63", "V": "8.88"})  # C passa a trazer a 63
    adapter.load(conn, c)
    depois = _ativas(conn, schema)
    # a 69 que a B1 trazia foi removida pela B2: não volta com o valor de 01/12
    assert (69, "202610") not in depois
    assert depois == _em(conn, schema, "2999-01-01")


# --------------------------------------------------------------------------
# I3: versão vazia ou truncada
# --------------------------------------------------------------------------
def test_resposta_so_com_cabecalho_com_sidecar_e_recusada_e_nada_muda(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a = arquivo_de_dados(raiz, "1737")
    adapter.load(conn, a)
    v2 = a.with_name(a.name.replace("@20261112", "@20261201"))
    v2.write_text(json.dumps(json.loads(a.read_text())[:1], ensure_ascii=False))
    side = json.loads(a.with_name(a.name + ".manifest.json").read_text())
    side["metadata"]["modificacao"] = "2026-12-01"
    v2.with_name(v2.name + ".manifest.json").write_text(json.dumps(side))
    with pytest.raises(SidraFonteError, match="nenhuma célula"):
        adapter.load(conn, v2)
    assert len(_ativas(conn, schema)) == 6
    assert _q(conn, schema, "SELECT count(*) FROM {s}.versao")[0][0] == 1
    # com --force a fonte esvaziou a consulta: a vigência fica sem células
    adapter.load(conn, v2, force=True)
    assert len(_ativas(conn, schema)) == 0


def test_arquivo_so_com_cabecalho_sem_sidecar_da_erro_claro(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a = arquivo_de_dados(raiz, "1737")
    b = nova_versao(a, "20261201", lambda r: None, sufixo="_vazio")
    with pytest.raises(SidraFonteError):
        adapter.load(conn, b)


def test_queda_de_mais_de_metade_das_celulas_e_recusada_sem_force(pg, tmp_path):
    adapter, conn, schema = pg
    assert QUEDA_MAXIMA == 0.5
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a = arquivo_de_dados(raiz, "1737")
    adapter.load(conn, a)  # 6 células
    metade = nova_versao(
        a, "20261201", lambda r: r if r["D2C"] in ("63", "69", "2263") else None
    )
    adapter.load(conn, metade)  # 3 de 6: exatamente 50%, passa
    pouco = nova_versao(a, "20261215", lambda r: r if r["D2C"] == "63" else None)
    with pytest.raises(SidraFonteError, match="queda de mais de 50%"):
        adapter.load(conn, pouco)
    assert len(_ativas(conn, schema)) == 3
    adapter.load(conn, pouco, force=True)
    assert len(_ativas(conn, schema)) == 1


# --------------------------------------------------------------------------
# I4: papel que só carrega
# --------------------------------------------------------------------------
def test_papel_que_nao_criou_o_schema_nem_e_dono_do_ledger_carrega(
    pg, pg_url, tmp_path
):
    import psycopg
    from psycopg.conninfo import make_conninfo

    adapter, conn, schema = pg
    papel = f"qsql_c_{uuid.uuid4().hex[:8]}"
    criar_esquema(conn, schema)  # o superusuário cria e é o dono de tudo
    conn.execute(f'CREATE ROLE "{papel}" LOGIN')
    try:
        conn.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{papel}"')
        conn.execute(
            f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "{schema}" '
            f'TO "{papel}"'
        )
        raiz = copia_tabelas(tmp_path / "bruto", "1737")
        with psycopg.connect(
            make_conninfo(pg_url, user=papel), autocommit=True
        ) as restrito:
            assert adapter.load(restrito, raiz, all_partitions=True) == 6
            assert adapter.load(restrito, raiz, all_partitions=True) == 0
        assert len(_ativas(conn, schema)) == 6
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.execute(f'DROP OWNED BY "{papel}"')
        conn.execute(f'DROP ROLE "{papel}"')


# --------------------------------------------------------------------------
# I5: a reconciliação não varre o fato
# --------------------------------------------------------------------------
def test_candidatas_da_reconciliacao_entram_pelo_indice_da_versao(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737", "7060")
    adapter.load(conn, raiz, all_partitions=True)
    conn.execute(
        "CREATE TEMP TABLE _qsql_sidra_afet AS SELECT serie_id, localidade_id, "
        f'periodo_id FROM "{schema}".observacao WHERE tabela_id = 1737'
    )
    conn.execute("SET enable_seqscan = off")
    plano = "\n".join(
        r[0]
        for r in conn.execute(
            ("EXPLAIN " + _SQL_CANDIDATAS).replace("{s}", f'"{schema}"'), ([1],)
        )
    )
    assert "Seq Scan on observacao" not in plano
    assert "observacao_versao_idx" in plano


# --------------------------------------------------------------------------
# M1: mesmo dia, hora do carimbo
# --------------------------------------------------------------------------
def test_versoes_do_mesmo_dia_se_ordenam_pela_hora_do_carimbo(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a = arquivo_de_dados(raiz, "1737")
    manha = nova_versao(
        a, "20261201T080000", lambda r: {**r, "V": "1.11"} if r["D2C"] == "63" else r
    )
    tarde = nova_versao(
        a, "20261201T150000", lambda r: {**r, "V": "2.22"} if r["D2C"] == "63" else r
    )
    adapter.load(conn, tarde)
    adapter.load(conn, manha)  # a de manhã chega depois, mas é mais antiga
    assert _ativas(conn, schema)[(63, "202610")] == "2.22"
    assert _um_ativo_por_celula(conn, schema) == 0
    assert _ativas(conn, schema) == _em(conn, schema, "2026-12-01")


# --------------------------------------------------------------------------
# M2: a ordem da classificação é a dos metadados
# --------------------------------------------------------------------------
def test_ordem_da_classificacao_vem_dos_metadados_e_nao_das_presentes(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1086")
    base = arquivo_de_dados(raiz, "1086")
    linhas = json.loads(base.read_text(encoding="utf-8"))
    # consulta só com a 2ª classificação (12529); a 1ª (12716) fica no total
    so = [linhas[0]] + [r for r in linhas[1:] if r["D4C"] == "115236"]
    cab = {k: v for k, v in so[0].items() if not k.startswith(("D4", "D5"))}
    cab.update({"D4C": so[0]["D5C"], "D4N": so[0]["D5N"]})
    novas = [cab] + [
        {
            **{k: v for k, v in r.items() if not k.startswith(("D4", "D5"))},
            "D4C": r["D5C"],
            "D4N": r["D5N"],
        }
        for r in so[1:]
    ]
    destino = base.with_name(base.name.replace("_c12716-all_c12529-all", "_c12529-all"))
    destino.write_text(json.dumps(novas, ensure_ascii=False), encoding="utf-8")
    adapter.load(conn, destino)
    d = _q(conn, schema, "SELECT DISTINCT d4c, d5c FROM {s}.dimensao")
    assert d and all(
        d4 is None and d5 is not None for d4, d5 in d
    )  # 2ª classificação em d5
    assert _q(conn, schema, "SELECT DISTINCT ordem FROM {s}.serie_categoria") == [(1,)]


# --------------------------------------------------------------------------
# M3: marca de versão do esquema
# --------------------------------------------------------------------------
def test_upgrade_da_marca_v1_recria_o_que_faltava(pg_schema):
    conn, schema = pg_schema
    criar_esquema(conn, schema)
    conn.execute(f'ALTER TABLE "{schema}".versao DROP COLUMN vigencia_em')
    conn.execute(
        f"COMMENT ON TABLE \"{schema}\".tabela IS 'quantilica-sql:sidra:esquema:v1'"
    )
    criar_esquema(conn, schema)  # marca antiga: a DDL roda de novo
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'versao'",
            (schema,),
        )
    }
    assert "vigencia_em" in cols
    marca = conn.execute(
        "SELECT obj_description(%s::regclass, 'pg_class')", (f'"{schema}".tabela',)
    ).fetchone()[0]
    assert marca.endswith(f"v{ESQUEMA_VERSAO}") and ESQUEMA_VERSAO == 2


def test_tabela_do_sidra_legado_no_schema_da_erro_claro(pg_schema):
    conn, schema = pg_schema
    conn.execute(f'CREATE SCHEMA "{schema}"')
    conn.execute(
        f'CREATE TABLE "{schema}".localidade (id bigint PRIMARY KEY, nc text, nn text, d1c text, d1n text)'
    )
    with pytest.raises(EsquemaAntigoError, match="'localidade'"):
        criar_esquema(conn, schema)
    conn.execute(f'DROP TABLE "{schema}".localidade')
    conn.execute(f'CREATE TABLE "{schema}".dimensao (id bigint PRIMARY KEY, mc text)')
    with pytest.raises(EsquemaAntigoError, match="'dimensao'"):
        criar_esquema(conn, schema)


# --------------------------------------------------------------------------
# M5, M6, M7
# --------------------------------------------------------------------------
def test_brin_com_autosummarize(pg_schema):
    conn, schema = pg_schema
    criar_esquema(conn, schema)
    opcoes = conn.execute(
        "SELECT reloptions FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = 'observacao_tabela_brin'",
        (schema,),
    ).fetchone()[0]
    assert "autosummarize=on" in opcoes


def test_renomeacao_de_variavel_categoria_e_classificacao_atualiza_o_nome(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "7060")
    adapter.load(conn, raiz, all_partitions=True)
    meta = next((raiz / "t-7060").glob("metadados@????????.json"))
    bruto = json.loads(meta.read_text(encoding="utf-8"))
    bruto["variaveis"][0]["nome"] = "IPCA - Variação mensal (renomeada)"
    bruto["classificacoes"][0]["nome"] = "Geral (renomeada)"
    bruto["classificacoes"][0]["categorias"][0]["nome"] = "Índice geral (renomeado)"
    novo = meta.with_name("metadados@20261201.json")
    novo.write_text(json.dumps(bruto, ensure_ascii=False), encoding="utf-8")
    arq = arquivo_de_dados(raiz, "7060")
    linhas = json.loads(arq.read_text(encoding="utf-8"))
    linhas[0]["D4N"] = "Geral (renomeada)"  # o IBGE renomeou também no cabeçalho
    arq.write_text(json.dumps(linhas, ensure_ascii=False), encoding="utf-8")
    adapter.load(conn, arq, force=True)
    assert _q(
        conn,
        schema,
        "SELECT nome FROM {s}.variavel WHERE id = %s",
        (bruto["variaveis"][0]["id"],),
    ) == [("IPCA - Variação mensal (renomeada)",)]
    assert _q(conn, schema, "SELECT nome FROM {s}.classificacao")[0] == (
        "Geral (renomeada)",
    )
    assert ("Índice geral (renomeado)",) in _q(
        conn, schema, "SELECT nome FROM {s}.categoria"
    )


def test_valor_vazio_vira_simbolo_vazio_e_as_sql_o_leem_como_nulo(pg, tmp_path):
    adapter, conn, schema = pg
    raiz = copia_tabelas(tmp_path / "bruto", "1737")
    a = arquivo_de_dados(raiz, "1737")
    reescreve(a, lambda r: {**r, "V": ""} if r["D2C"] == "69" else r)
    adapter.load(conn, a)
    assert _q(
        conn,
        schema,
        "SELECT simbolo, valor FROM {s}.observacao WHERE simbolo IS NOT NULL",
    ) == [("", None)]
    # o padrão do catálogo: CASE WHEN D.V ~ '^-?[0-9]' THEN D.V::NUMERIC END
    lido = _q(
        conn,
        schema,
        "SELECT CASE WHEN v ~ '^-?[0-9]' THEN v::numeric END FROM {s}.dados "
        "WHERE dimensao_id = (SELECT id FROM {s}.serie WHERE variavel_id = 69)",
    )
    assert lido == [(None,)]


@pytest.mark.parametrize("texto", ["1,5", "1e-3", "+1", ".5", "1."])
def test_numero_fora_do_formato_da_api_para_a_carga(texto):
    from quantilica_sql.sidra.valores import converter_valor

    with pytest.raises(SidraFonteError):
        converter_valor(texto)
