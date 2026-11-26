"""Esquema dimensional do SIDRA: DDL, superfície do legado, view plana, esquema antigo.

Exige ``QUANTILICA_TEST_DATABASE_URL`` (banco descartável); cada teste usa um schema
próprio, removido ao final.
"""

from __future__ import annotations

import re
import uuid

import pytest

from quantilica_sql.sidra.esquema import (
    FUNCOES,
    TABELA_ANTIGA,
    TABELAS,
    VIEWS,
    EsquemaAntigoError,
    criar_esquema,
    esquema_antigo,
    preparar_esquema,
    recriar_esquema,
    resumo,
)


def _um(conn, query, params=None):
    return conn.execute(query, params).fetchone()


def _colunas(conn, schema, objeto):
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s",
        (schema, objeto),
    ).fetchall()
    return dict(rows)


def popula(conn, schema):
    """Um conjunto mínimo e coerente, escrito à mão (a carga real é da frente 3)."""
    s = f'"{schema}"'
    conn.execute(f"INSERT INTO {s}.tabela (id, nome) VALUES (7060, 'IPCA por itens')")
    conn.execute(
        f"INSERT INTO {s}.variavel VALUES (7060, 63, 'IPCA - Variação mensal')"
    )
    conn.execute(f"INSERT INTO {s}.classificacao VALUES (7060, 315, 'Geral', 0)")
    conn.execute(
        f"INSERT INTO {s}.categoria VALUES (7060, 315, 7169, 'Índice geral', 1, NULL)"
    )
    conn.execute(
        f"INSERT INTO {s}.localidade (nivel_codigo, nivel_nome, codigo, nome) "
        "VALUES ('1', 'Brasil', '1', 'Brasil')"
    )
    conn.execute(
        f"INSERT INTO {s}.periodo (frequencia, codigo, rotulo, ano, mes, data_inicio, data_fim) "
        "VALUES ('mensal', '202610', 'outubro 2026', 2026, 10, '2026-10-01', '2026-10-31')"
    )
    conn.execute(
        f"INSERT INTO {s}.serie (tabela_id, variavel_id, unidade_codigo, unidade_nome, chave) "
        "VALUES (7060, 63, '2', '%', '63|2|315:7169')"
    )
    conn.execute(f"INSERT INTO {s}.serie_categoria VALUES (1, 0, 7060, 315, 7169)")
    conn.execute(
        f"INSERT INTO {s}.versao (tabela_id, consulta, modificacao, arquivo, linhas) "
        "VALUES (7060, 'c', '2026-11-12', 'a.json', 2)"
    )
    conn.execute(
        f"INSERT INTO {s}.observacao (versao_id, tabela_id, serie_id, localidade_id, "
        "periodo_id, valor, simbolo, ativo) VALUES (1, 7060, 1, 1, 1, 0.14, NULL, true)"
    )


# --------------------------------------------------------------------------
# DDL
# --------------------------------------------------------------------------
def test_ddl_criar_duas_vezes_nao_falha_e_bate_com_o_adr(pg_schema):
    conn, schema = pg_schema
    criar_esquema(conn, schema)
    criar_esquema(conn, schema)
    tabelas = {
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE'",
            (schema,),
        )
    }
    views = {
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.views WHERE table_schema = %s",
            (schema,),
        )
    }
    assert tabelas == set(TABELAS)
    assert views == set(VIEWS)
    funcoes = {
        r[0]
        for r in conn.execute(
            "SELECT routine_name FROM information_schema.routines "
            "WHERE routine_schema = %s",
            (schema,),
        )
    }
    assert funcoes == set(FUNCOES)
    obs = _colunas(conn, schema, "observacao")
    assert obs == {
        "versao_id": "integer",
        "tabela_id": "integer",
        "serie_id": "integer",
        "localidade_id": "integer",
        "periodo_id": "integer",
        "valor": "numeric",
        "simbolo": "text",
        "ativo": "boolean",
    }
    assert set(_colunas(conn, schema, "versao")) >= {
        "tabela_id",
        "consulta",
        "modificacao",
        "arquivo",
        "sha256",
        "linhas",
    }
    assert set(_colunas(conn, schema, "arquivo_carregado")) >= {"sha256", "dataset"}


def test_ddl_esquema_marcado_nao_reexecuta_a_ddl_e_forcar_refaz(pg_schema):
    conn, schema = pg_schema
    criar_esquema(conn, schema)
    conn.execute(f'DROP VIEW "{schema}".observacao_plana')
    criar_esquema(conn, schema)  # marcado: não mexe (o papel pode não ser o dono)
    assert _relkind(conn, schema, "observacao_plana") is None
    criar_esquema(conn, schema, forcar=True)
    assert _relkind(conn, schema, "observacao_plana") == "v"


def _relkind(conn, schema, nome):
    row = conn.execute(
        "SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s",
        (schema, nome),
    ).fetchone()
    return None if row is None else row[0]


def test_ddl_valor_ou_simbolo_e_unico_entre_as_ativas_e_versao_tem_indice(pg_schema):
    import psycopg

    conn, schema = pg_schema
    criar_esquema(conn, schema)
    popula(conn, schema)
    s = f'"{schema}"'
    ins = (
        f"INSERT INTO {s}.observacao (versao_id, tabela_id, serie_id, localidade_id, "
        "periodo_id, valor, simbolo, ativo) VALUES (1, 7060, 1, 1, 1, %s, %s, %s)"
    )
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(ins, (1, "-", False))  # número e símbolo
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(ins, (None, None, False))  # nenhum dos dois
    with pytest.raises(psycopg.errors.UniqueViolation):
        conn.execute(ins, (2, None, True))  # segunda ativa na mesma célula
    conn.execute(ins, (2, None, False))  # inativa repetida é história: pode
    conn.execute(ins, (None, "...", False))
    indices = {
        r[0]
        for r in conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = %s "
            "AND tablename = 'observacao'",
            (schema,),
        )
    }
    assert any("(versao_id)" in d for d in indices)
    assert any("UNIQUE" in d and "WHERE ativo" in d for d in indices)


# --------------------------------------------------------------------------
# Superfície do legado
# --------------------------------------------------------------------------
LEGADO = {
    "dados": {
        "tabela_sidra_id": "text",
        "localidade_id": "integer",
        "dimensao_id": "integer",
        "periodo_id": "integer",
        "modificacao": "date",
        "ativo": "boolean",
        "v": "text",
    },
    "periodo": {
        "id": "integer",
        "ano": "integer",
        "trimestre": "smallint",
        "mes": "smallint",
        "frequencia": "text",
        "data_inicio": "date",
    },
    "localidade": {
        "id": "integer",
        "nc": "text",
        "nn": "text",
        "d1c": "text",
        "d1n": "text",
    },
    "dimensao": {
        "id": "integer",
        "mc": "text",
        "mn": "text",
        "d2c": "text",
        "d2n": "text",
        **{f"d{n}{x}": "text" for n in range(4, 10) for x in "cn"},
    },
}


def test_legado_cada_coluna_das_51_sql_existe_com_o_tipo(pg_schema):
    conn, schema = pg_schema
    criar_esquema(conn, schema)
    for objeto, esperadas in LEGADO.items():
        reais = _colunas(conn, schema, objeto)
        for coluna, tipo in esperadas.items():
            assert reais.get(coluna) == tipo, f"{objeto}.{coluna}: {reais.get(coluna)}"


def test_legado_dados_dimensao_localidade_e_periodo_leem_o_conjunto(pg_schema):
    conn, schema = pg_schema
    criar_esquema(conn, schema)
    popula(conn, schema)
    s = f'"{schema}"'
    conn.execute(
        f"INSERT INTO {s}.observacao (versao_id, tabela_id, serie_id, localidade_id, "
        "periodo_id, valor, simbolo, ativo) VALUES (1, 7060, 1, 1, 1, NULL, '..', false)"
    )
    assert _um(
        conn,
        f"SELECT D.tabela_sidra_id, D.v, D.modificacao, D.ativo FROM {s}.dados D "
        "WHERE D.ativo",
    ) == ("7060", "0.14", __import__("datetime").date(2026, 11, 12), True)
    assert _um(conn, f"SELECT v FROM {s}.dados WHERE NOT ativo") == ("..",)
    dim = conn.execute(
        f"SELECT mc, mn, d2c, d2n, d4c, d4n, d5c, d5n FROM {s}.dimensao"
    ).fetchall()
    assert dim == [
        ("2", "%", "63", "IPCA - Variação mensal", "7169", "Índice geral", None, None)
    ]
    assert _um(conn, f"SELECT nc, nn, d1c, d1n FROM {s}.localidade") == (
        "1",
        "Brasil",
        "1",
        "Brasil",
    )
    assert _um(conn, f"SELECT ano, mes, trimestre, frequencia FROM {s}.periodo") == (
        2026,
        10,
        None,
        "mensal",
    )
    # a receita do catálogo: JOIN pelos ids da superfície do legado
    assert _um(
        conn,
        f"SELECT P.data_inicio, L.d1n, DIM.d4n, D.v FROM {s}.dados D "
        f"JOIN {s}.periodo P ON D.periodo_id = P.id "
        f"JOIN {s}.dimensao DIM ON D.dimensao_id = DIM.id "
        f"JOIN {s}.localidade L ON D.localidade_id = L.id "
        "WHERE D.tabela_sidra_id IN ('7060') AND D.ativo = TRUE",
    )[1:] == ("Brasil", "Índice geral", "0.14")


def test_legado_dados_sem_modificacao_nao_junta_versao(pg_schema):
    conn, schema = pg_schema
    criar_esquema(conn, schema)
    s = f'"{schema}"'
    sem = "\n".join(
        r[0]
        for r in conn.execute(f"EXPLAIN SELECT tabela_sidra_id, ativo FROM {s}.dados")
    )
    com = "\n".join(
        r[0]
        for r in conn.execute(
            f"EXPLAIN SELECT tabela_sidra_id, modificacao FROM {s}.dados"
        )
    )
    assert "versao" not in sem
    assert "versao" in com


def test_legado_filtro_por_tabela_sidra_id_pode_usar_o_indice_brin(pg_schema):
    conn, schema = pg_schema
    criar_esquema(conn, schema)
    popula(conn, schema)
    conn.execute("SET enable_seqscan = off")
    plano = "\n".join(
        r[0]
        for r in conn.execute(
            f"EXPLAIN SELECT v FROM \"{schema}\".dados WHERE tabela_sidra_id IN ('7060')"
        )
    )
    assert "observacao_tabela_brin" in plano


def test_legado_dimensao_com_seis_classificacoes(pg_schema):
    conn, schema = pg_schema
    criar_esquema(conn, schema)
    s = f'"{schema}"'
    conn.execute(f"INSERT INTO {s}.tabela (id, nome) VALUES (1, 't')")
    conn.execute(f"INSERT INTO {s}.variavel VALUES (1, 9, 'v')")
    conn.execute(
        f"INSERT INTO {s}.serie (tabela_id, variavel_id, unidade_codigo, unidade_nome, chave) "
        "VALUES (1, 9, '1', 'u', 'k')"
    )
    for k in range(6):
        conn.execute(
            f"INSERT INTO {s}.classificacao VALUES (1, {100 + k}, 'c{k}', {k})"
        )
        conn.execute(
            f"INSERT INTO {s}.categoria VALUES (1, {100 + k}, {k}, 'n{k}', 1, NULL)"
        )
        conn.execute(
            f"INSERT INTO {s}.serie_categoria VALUES (1, {k}, 1, {100 + k}, {k})"
        )
    linha = _um(conn, f"SELECT d4c, d4n, d9c, d9n FROM {s}.dimensao")
    assert linha == ("0", "n0", "5", "n5")


# --------------------------------------------------------------------------
# View plana
# --------------------------------------------------------------------------
def test_plana_tem_os_nomes_e_as_categorias_em_jsonb(pg_schema):
    conn, schema = pg_schema
    criar_esquema(conn, schema)
    popula(conn, schema)
    s = f'"{schema}"'
    cols = list(_colunas(conn, schema, "observacao_plana"))
    assert set(cols) == {
        "tabela_id",
        "tabela",
        "variavel_id",
        "variavel",
        "unidade_codigo",
        "unidade",
        "nivel_codigo",
        "nivel",
        "localidade_codigo",
        "localidade",
        "frequencia",
        "periodo_codigo",
        "periodo",
        "data_inicio",
        "data_fim",
        "categorias",
        "valor",
        "simbolo",
        "modificacao",
        "ativo",
    }
    linha = conn.execute(
        f"SELECT tabela, variavel, unidade, nivel, localidade, periodo, categorias, "
        f"valor, simbolo, ativo FROM {s}.observacao_plana"
    ).fetchall()
    assert len(linha) == 1
    (
        tabela,
        variavel,
        unidade,
        nivel,
        localidade,
        periodo,
        cats,
        valor,
        simbolo,
        ativo,
    ) = linha[0]
    assert (tabela, variavel, unidade, nivel, localidade, periodo) == (
        "IPCA por itens",
        "IPCA - Variação mensal",
        "%",
        "Brasil",
        "Brasil",
        "outubro 2026",
    )
    assert cats == {"Geral": "Índice geral"}
    assert (str(valor), simbolo, ativo) == ("0.14", None, True)


# --------------------------------------------------------------------------
# Esquema antigo
# --------------------------------------------------------------------------
def _cria_antigo(conn, schema):
    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    conn.execute(
        f"""CREATE TABLE "{schema}".dados (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            tabela_sidra_id TEXT NOT NULL, localidade_id BIGINT NOT NULL,
            dimensao_id BIGINT NOT NULL, periodo_id INTEGER NOT NULL,
            modificacao DATE NOT NULL DEFAULT CURRENT_DATE,
            ativo BOOLEAN NOT NULL DEFAULT TRUE, v TEXT NOT NULL)"""
    )
    conn.execute(
        f"INSERT INTO \"{schema}\".dados (tabela_sidra_id, localidade_id, dimensao_id, periodo_id, v) VALUES ('1086', 1, 2, 3, '4.5')"
    )


def _ledger(conn, schema, dataset, nome):
    from quantilica_sql.core.tracker import ensure_tracker_table

    ensure_tracker_table(conn, schema)
    conn.execute(
        f'INSERT INTO "{schema}".arquivo_carregado (nome_arquivo, dataset, particao, linhas) '
        "VALUES (%s, %s, '1086', 1)",
        (nome, dataset),
    )


def test_antigo_dados_de_0_3_x_para_com_o_remedio(pg_schema):
    conn, schema = pg_schema
    _cria_antigo(conn, schema)
    assert esquema_antigo(conn, schema)
    for chamada in (
        lambda: criar_esquema(conn, schema),
        lambda: preparar_esquema(conn, schema),
    ):
        with pytest.raises(EsquemaAntigoError) as exc:
            chamada()
        msg = str(exc.value)
        assert "--recriar" in msg and TABELA_ANTIGA in msg and schema in msg
    # nada foi tocado
    assert _um(conn, f'SELECT count(*) FROM "{schema}".dados') == (1,)


def test_antigo_recriar_renomeia_apaga_so_o_ledger_do_sidra_e_cria_o_novo(pg_schema):
    conn, schema = pg_schema
    _cria_antigo(conn, schema)
    _ledger(conn, schema, "sidra", "velho.json")
    _ledger(conn, schema, "outra-fonte", "fica.json")
    preparar_esquema(conn, schema, recriar=True)
    assert not esquema_antigo(conn, schema)
    assert _um(conn, f'SELECT count(*), min(v) FROM "{schema}".{TABELA_ANTIGA}') == (
        1,
        "4.5",
    )
    assert _um(conn, f'SELECT count(*) FROM "{schema}".dados') == (0,)  # a view nova
    assert conn.execute(
        f'SELECT dataset FROM "{schema}".arquivo_carregado'
    ).fetchall() == [("outra-fonte",)]
    assert _um(
        conn,
        "SELECT relkind FROM pg_class WHERE oid = %s::regclass",
        (f'"{schema}".dados',),
    ) == ("v",)
    # sem esquema antigo, recriar é o mesmo que criar (e não renomeia nada)
    preparar_esquema(conn, schema, recriar=True)
    assert _um(conn, f'SELECT count(*) FROM "{schema}".{TABELA_ANTIGA}') == (1,)


def test_antigo_recriar_nao_sobrescreve_uma_recriacao_anterior(pg_schema):
    conn, schema = pg_schema
    _cria_antigo(conn, schema)
    conn.execute(f'CREATE TABLE "{schema}".{TABELA_ANTIGA} (x int)')
    with pytest.raises(EsquemaAntigoError, match=TABELA_ANTIGA):
        recriar_esquema(conn, schema)
    assert esquema_antigo(conn, schema)


def test_antigo_schema_vazio_cria_direto(pg_schema):
    conn, schema = pg_schema
    assert not esquema_antigo(conn, schema)
    preparar_esquema(conn, schema)
    assert resumo(conn, schema) == {
        "tabelas": 0,
        "versoes": 0,
        "observacoes": 0,
        "observacoes_ativas": 0,
    }


# --------------------------------------------------------------------------
# Papel sem CREATE no banco
# --------------------------------------------------------------------------
def test_papel_restrito_so_com_usage_e_create_no_schema(pg_schema, pg_url):
    import psycopg
    from psycopg.conninfo import make_conninfo

    conn, schema = pg_schema
    papel = f"qsql_r_{uuid.uuid4().hex[:8]}"
    conn.execute(f'CREATE SCHEMA "{schema}"')
    conn.execute(f"CREATE ROLE \"{papel}\" LOGIN PASSWORD '{papel}'")
    try:
        conn.execute(f'GRANT USAGE, CREATE ON SCHEMA "{schema}" TO "{papel}"')
        banco = _um(conn, "SELECT current_database()")[0]
        assert _um(
            conn, "SELECT has_database_privilege(%s, %s, 'CREATE')", (papel, banco)
        ) == (False,)
        with psycopg.connect(
            make_conninfo(pg_url, user=papel, password=papel), autocommit=True
        ) as restrito:
            criar_esquema(restrito, schema)
            criar_esquema(restrito, schema)
            assert resumo(restrito, schema)["observacoes"] == 0
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.execute(f'DROP OWNED BY "{papel}"')
        conn.execute(f'DROP ROLE "{papel}"')


# --------------------------------------------------------------------------
# status
# --------------------------------------------------------------------------
def test_status_do_adaptador_conta_tabelas_versoes_observacoes_e_ativas(pg_schema):
    from quantilica_sql.adapters.sidra import SidraAdapter

    conn, schema = pg_schema

    class _Sidra(SidraAdapter):
        @property
        def schema(self) -> str:
            return schema

    adapter = _Sidra()
    vazio = adapter.status(conn)
    assert (vazio["tabelas_carregadas"], vazio["observacoes"]) == (0, 0)
    assert vazio["esquema_antigo"] is False
    criar_esquema(conn, schema)
    popula(conn, schema)
    s = f'"{schema}"'
    conn.execute(
        f"INSERT INTO {s}.observacao (versao_id, tabela_id, serie_id, localidade_id, "
        "periodo_id, valor, simbolo, ativo) VALUES (1, 7060, 1, 1, 1, 9, NULL, false)"
    )
    st = adapter.status(conn)
    assert (
        st["tabelas_carregadas"],
        st["versoes"],
        st["observacoes"],
        st["observacoes_ativas"],
    ) == (1, 1, 2, 1)
    assert st["domain"] == "sidra" and st["schema"] == schema


def test_status_do_cli_continua_listando_o_schema(pg_schema, monkeypatch):
    from typer.testing import CliRunner

    from quantilica_sql import cli

    conn, schema = pg_schema
    criar_esquema(conn, schema)

    class _Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(cli, "get_connection", lambda **kw: _Ctx())
    res = CliRunner().invoke(cli.app, ["status"], env={"COLUMNS": "200"})
    assert res.exit_code == 0, res.output
    linha = next(ln for ln in res.output.splitlines() if schema in ln)
    # 11 tabelas de base (as views não contam em pg_tables)
    assert re.search(rf"{schema}\s+│?\s*{len(TABELAS)}\b", linha), linha
