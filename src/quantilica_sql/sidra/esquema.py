"""Esquema dimensional do SIDRA (``ibge_sidra``): DDL idempotente e esquema antigo.

Modelo estrela em formato longo (ADR de 25/11/2026):

* **fato** ``observacao``: uma linha por célula e versão (``valor`` numérico exato ou
  ``simbolo`` da fonte; ``ativo`` marca a única vigente de cada célula);
* **série** (``serie`` + ponte ``serie_categoria``): variável, unidade e a combinação
  de categorias; a ponte para as N classificações fica na série, nunca no fato;
* dimensões ``tabela``, ``variavel``, ``classificacao``, ``categoria`` (por tabela),
  ``localidade`` e ``periodo``; ``versao`` (um arquivo = uma versão) e o ledger
  ``arquivo_carregado``;
* **superfície do legado, congelada** (as SQL do catálogo e o gerador do portal leem
  assim): ``localidade`` com ``nc``, ``nn``, ``d1c``, ``d1n``; as views ``dimensao``
  (``mc``, ``mn``, ``d2c``, ``d2n``, ``d4c``..``d9n``) e ``dados``
  (``tabela_sidra_id``, ``localidade_id``, ``dimensao_id``, ``periodo_id``,
  ``modificacao``, ``ativo``, ``v``);
* a view ``observacao_plana``, com os nomes, para quem consulta sem JOIN, e a função
  ``dados_em(data)``: o que valia numa data (a versão de maior ``modificacao`` até
  ela em cada célula; versão sem data não entra).

Nenhuma DDL exige ``CREATE`` no banco quando o schema já existe (só no schema).
Exige PostgreSQL 12+ (colunas geradas ``STORED``).
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.core.tracker import ensure_tracker_table
from quantilica_sql.core.txscope import commit_if_standalone
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.sidra.esquema")

DATASET = "sidra"  # ``dataset`` do ledger ``arquivo_carregado``
TABELA_ANTIGA = "dados_legado_v0"
ESQUEMA_VERSAO = 2  # sobe quando a DDL muda; fica no COMMENT de ``tabela``
_MARCA = f"quantilica-sql:sidra:esquema:v{ESQUEMA_VERSAO}"
MAX_CLASSIFICACOES_NA_VIEW = 6  # d4..d9, o limite que o portal já declara

TABELAS = (
    "tabela",
    "variavel",
    "classificacao",
    "categoria",
    "localidade",
    "periodo",
    "serie",
    "serie_categoria",
    "versao",
    "observacao",
    "arquivo_carregado",
)
VIEWS = ("dimensao", "dados", "observacao_plana")
FUNCOES = ("dados_em",)


class EsquemaAntigoError(RuntimeError):
    """O schema tem o ``dados`` do 0.3.x (tabela de base): precisa de ``--recriar``."""


_DDL_TABELAS = """
CREATE TABLE IF NOT EXISTS {s}.tabela (
    id               integer     PRIMARY KEY,
    nome             text        NOT NULL,
    pesquisa         text,
    assunto          text,
    periodicidade    text,
    url              text,
    metadados        jsonb       NOT NULL DEFAULT '{{}}'::jsonb,
    metadados_arquivo text,
    atualizado_em    timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS {s}.variavel (
    tabela_id integer NOT NULL REFERENCES {s}.tabela (id),
    id        integer NOT NULL,
    nome      text    NOT NULL,
    PRIMARY KEY (tabela_id, id)
);

CREATE TABLE IF NOT EXISTS {s}.classificacao (
    tabela_id integer  NOT NULL REFERENCES {s}.tabela (id),
    id        integer  NOT NULL,
    nome      text     NOT NULL,
    ordem     smallint NOT NULL,
    PRIMARY KEY (tabela_id, id)
);

CREATE TABLE IF NOT EXISTS {s}.categoria (
    tabela_id        integer  NOT NULL,
    classificacao_id integer  NOT NULL,
    id               integer  NOT NULL,
    nome             text     NOT NULL,
    nivel            smallint,
    unidade          text,
    PRIMARY KEY (tabela_id, classificacao_id, id),
    FOREIGN KEY (tabela_id, classificacao_id)
        REFERENCES {s}.classificacao (tabela_id, id)
);

CREATE TABLE IF NOT EXISTS {s}.localidade (
    id            integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    nivel_codigo  text    NOT NULL,
    nivel_nome    text    NOT NULL,
    codigo        text    NOT NULL,
    nome          text    NOT NULL,
    nc            text    GENERATED ALWAYS AS (nivel_codigo) STORED,
    nn            text    GENERATED ALWAYS AS (nivel_nome) STORED,
    d1c           text    GENERATED ALWAYS AS (codigo) STORED,
    d1n           text    GENERATED ALWAYS AS (nome) STORED,
    UNIQUE (nivel_codigo, codigo)
);

CREATE TABLE IF NOT EXISTS {s}.periodo (
    id          integer  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    frequencia  text     NOT NULL,
    codigo      text     NOT NULL,
    rotulo      text,
    ano         integer,
    mes         smallint,
    trimestre   smallint,
    semestre    smallint,
    data_inicio date,
    data_fim    date,
    UNIQUE (frequencia, codigo)
);

CREATE TABLE IF NOT EXISTS {s}.serie (
    id             integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tabela_id      integer NOT NULL,
    variavel_id    integer NOT NULL,
    unidade_codigo text    NOT NULL,
    unidade_nome   text    NOT NULL,
    chave          text    NOT NULL,
    UNIQUE (tabela_id, chave),
    FOREIGN KEY (tabela_id, variavel_id) REFERENCES {s}.variavel (tabela_id, id)
);

CREATE TABLE IF NOT EXISTS {s}.serie_categoria (
    serie_id         integer  NOT NULL REFERENCES {s}.serie (id),
    ordem            smallint NOT NULL,
    tabela_id        integer  NOT NULL,
    classificacao_id integer  NOT NULL,
    categoria_id     integer  NOT NULL,
    PRIMARY KEY (serie_id, ordem),
    FOREIGN KEY (tabela_id, classificacao_id, categoria_id)
        REFERENCES {s}.categoria (tabela_id, classificacao_id, id)
);

CREATE TABLE IF NOT EXISTS {s}.versao (
    id           integer     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tabela_id    integer     NOT NULL REFERENCES {s}.tabela (id),
    consulta     text        NOT NULL,
    modificacao  date,
    vigencia_em  timestamp,
    arquivo      text        NOT NULL,
    sha256       text,
    linhas       bigint      NOT NULL DEFAULT 0,
    periodo_ids  integer[]   NOT NULL DEFAULT '{{}}'::integer[],
    carregado_em timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tabela_id, arquivo)
);
-- esquema v1 não tinha estas colunas
ALTER TABLE {s}.versao ADD COLUMN IF NOT EXISTS vigencia_em timestamp;
ALTER TABLE {s}.versao
    ADD COLUMN IF NOT EXISTS periodo_ids integer[] NOT NULL DEFAULT '{{}}'::integer[];
CREATE INDEX IF NOT EXISTS versao_consulta_idx
    ON {s}.versao (tabela_id, consulta);

-- Sem chaves estrangeiras no fato: a FK de versao_id mais que dobra o custo do
-- INSERT (medido); a integridade é do carregador (uma transação por arquivo).
CREATE TABLE IF NOT EXISTS {s}.observacao (
    versao_id     integer NOT NULL,
    tabela_id     integer NOT NULL,
    serie_id      integer NOT NULL,
    localidade_id integer NOT NULL,
    periodo_id    integer NOT NULL,
    valor         numeric,
    simbolo       text,
    ativo         boolean NOT NULL DEFAULT true,
    CONSTRAINT observacao_valor_ou_simbolo
        CHECK ((valor IS NULL) <> (simbolo IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS observacao_ativa_uq
    ON {s}.observacao (serie_id, localidade_id, periodo_id) WHERE ativo;
CREATE INDEX IF NOT EXISTS observacao_versao_idx
    ON {s}.observacao (versao_id);
-- As SQL do catálogo filtram por ``tabela_sidra_id`` (texto, a view de ``dados``):
-- um BRIN sobre a mesma expressão as serve (as tabelas entram em blocos contíguos)
-- sem custo mensurável na carga.
CREATE INDEX IF NOT EXISTS observacao_tabela_brin
    ON {s}.observacao USING brin ((tabela_id::text)) WITH (autosummarize = on);
ALTER INDEX {s}.observacao_tabela_brin SET (autosummarize = on);
"""


def _view_dimensao() -> str:
    selects = []
    joins = []
    for k in range(MAX_CLASSIFICACOES_NA_VIEW):
        n = 4 + k
        selects.append(f"c{k}.id::text AS d{n}c")
        selects.append(f"c{k}.nome AS d{n}n")
        joins.append(
            f"LEFT JOIN {{s}}.serie_categoria sc{k} "
            f"ON sc{k}.serie_id = s.id AND sc{k}.ordem = {k} "
            f"LEFT JOIN {{s}}.categoria c{k} "
            f"ON c{k}.tabela_id = sc{k}.tabela_id "
            f"AND c{k}.classificacao_id = sc{k}.classificacao_id "
            f"AND c{k}.id = sc{k}.categoria_id"
        )
    return (
        "CREATE OR REPLACE VIEW {s}.dimensao AS "
        "SELECT s.id, s.unidade_codigo AS mc, s.unidade_nome AS mn, "
        "s.variavel_id::text AS d2c, v.nome AS d2n, "
        + ", ".join(selects)
        + " FROM {s}.serie s JOIN {s}.variavel v "
        "ON v.tabela_id = s.tabela_id AND v.id = s.variavel_id " + " ".join(joins) + ";"
    )


_DDL_VIEWS = (
    _view_dimensao()
    + """
CREATE OR REPLACE VIEW {s}.dados AS
SELECT o.tabela_id::text                AS tabela_sidra_id,
       o.localidade_id                  AS localidade_id,
       o.serie_id                       AS dimensao_id,
       o.periodo_id                     AS periodo_id,
       ve.modificacao                   AS modificacao,
       o.ativo                          AS ativo,
       COALESCE(o.valor::text, o.simbolo) AS v
FROM {s}.observacao o
LEFT JOIN {s}.versao ve ON ve.id = o.versao_id;

CREATE OR REPLACE VIEW {s}.observacao_plana AS
SELECT o.tabela_id,
       t.nome                  AS tabela,
       s.variavel_id,
       v.nome                  AS variavel,
       s.unidade_codigo,
       s.unidade_nome          AS unidade,
       l.nivel_codigo,
       l.nivel_nome            AS nivel,
       l.codigo                AS localidade_codigo,
       l.nome                  AS localidade,
       p.frequencia,
       p.codigo                AS periodo_codigo,
       p.rotulo                AS periodo,
       p.data_inicio,
       p.data_fim,
       COALESCE((SELECT jsonb_object_agg(cl.nome, ca.nome)
                 FROM {s}.serie_categoria sc
                 JOIN {s}.classificacao cl
                   ON cl.tabela_id = sc.tabela_id AND cl.id = sc.classificacao_id
                 JOIN {s}.categoria ca
                   ON ca.tabela_id = sc.tabela_id
                  AND ca.classificacao_id = sc.classificacao_id
                  AND ca.id = sc.categoria_id
                 WHERE sc.serie_id = s.id), '{{}}'::jsonb) AS categorias,
       o.valor,
       o.simbolo,
       ve.modificacao,
       o.ativo
FROM {s}.observacao o
JOIN {s}.tabela t ON t.id = o.tabela_id
JOIN {s}.serie s ON s.id = o.serie_id
JOIN {s}.variavel v ON v.tabela_id = s.tabela_id AND v.id = s.variavel_id
JOIN {s}.localidade l ON l.id = o.localidade_id
JOIN {s}.periodo p ON p.id = o.periodo_id
LEFT JOIN {s}.versao ve ON ve.id = o.versao_id;

-- O que valia numa data: de cada consulta, a última versão até ela (uma revisão
-- que removeu a célula a tira); entre consultas vence a de maior vigencia_em e, no
-- empate, a primeira carregada. Mesma regra do ``ativo``. Versão sem data não entra.
-- Dê a tabela (``dados_em(data, '1737')``) para ler só as versões dela; sem ela a
-- função percorre o fato inteiro.
CREATE OR REPLACE FUNCTION {s}.dados_em(data date, tabela text)
RETURNS TABLE (
    tabela_sidra_id text,
    localidade_id   integer,
    dimensao_id     integer,
    periodo_id      integer,
    modificacao     date,
    ativo           boolean,
    v               text
)
LANGUAGE sql STABLE AS $fn$
    WITH ultimas AS (
        SELECT DISTINCT ON (ve.tabela_id, ve.consulta)
               ve.id, ve.tabela_id, ve.modificacao, ve.vigencia_em
        FROM {s}.versao ve
        WHERE ve.modificacao <= data AND ve.tabela_id = tabela::integer
        ORDER BY ve.tabela_id, ve.consulta, ve.vigencia_em DESC, ve.id DESC
    )
    SELECT DISTINCT ON (o.serie_id, o.localidade_id, o.periodo_id)
           o.tabela_id::text, o.localidade_id, o.serie_id, o.periodo_id,
           u.modificacao, true, COALESCE(o.valor::text, o.simbolo)
    FROM ultimas u
    JOIN {s}.observacao o ON o.versao_id = u.id
    ORDER BY o.serie_id, o.localidade_id, o.periodo_id, u.vigencia_em DESC, u.id ASC
$fn$;

CREATE OR REPLACE FUNCTION {s}.dados_em(data date)
RETURNS TABLE (
    tabela_sidra_id text,
    localidade_id   integer,
    dimensao_id     integer,
    periodo_id      integer,
    modificacao     date,
    ativo           boolean,
    v               text
)
LANGUAGE sql STABLE AS $fn$
    WITH ultimas AS (
        SELECT DISTINCT ON (ve.tabela_id, ve.consulta)
               ve.id, ve.tabela_id, ve.modificacao, ve.vigencia_em
        FROM {s}.versao ve
        WHERE ve.modificacao <= data
        ORDER BY ve.tabela_id, ve.consulta, ve.vigencia_em DESC, ve.id DESC
    )
    SELECT DISTINCT ON (o.serie_id, o.localidade_id, o.periodo_id)
           o.tabela_id::text, o.localidade_id, o.serie_id, o.periodo_id,
           u.modificacao, true, COALESCE(o.valor::text, o.simbolo)
    FROM ultimas u
    JOIN {s}.observacao o ON o.versao_id = u.id
    ORDER BY o.serie_id, o.localidade_id, o.periodo_id, u.vigencia_em DESC, u.id ASC
$fn$;
"""
)


def _relkind(conn: psycopg.Connection, schema: str, nome: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relkind FROM pg_class c JOIN pg_namespace n "
            "ON n.oid = c.relnamespace WHERE n.nspname = %s AND c.relname = %s",
            (schema, nome),
        )
        row = cur.fetchone()
    return None if row is None else str(row[0])


def esquema_antigo(conn: psycopg.Connection, schema: str) -> bool:
    """``True`` se ``<schema>.dados`` é uma tabela de base (o esquema do 0.3.x)."""
    return _relkind(conn, schema, "dados") in ("r", "p")


def mensagem_esquema_antigo(schema: str) -> str:
    return (
        f"o schema {schema!r} tem o esquema antigo do SIDRA (a tabela {schema}.dados "
        "do quantilica-sql 0.3.x, sem séries, classificações nem versões), que não "
        "se converte para o modelo novo. Rode a carga com --recriar: a tabela antiga "
        f"vira {schema}.{TABELA_ANTIGA}, o registro de cargas do SIDRA é apagado e o "
        "esquema novo é criado; depois carregue de novo."
    )


_COLUNAS_CHAVE = {
    "localidade": {"nivel_codigo", "codigo"},
    "periodo": {"frequencia", "codigo"},
    "tabela": {"metadados"},
    "versao": {"consulta", "arquivo"},
    "observacao": {"serie_id", "versao_id"},
}


def _conferir_objetos_alheios(conn: psycopg.Connection, schema: str) -> None:
    """Recusa, com mensagem clara, objeto de outro esquema com o nome de um nosso.

    O ``sidra-sql`` legado também tinha ``localidade``, ``periodo`` e ``dimensao``
    (tabelas) no schema; ``CREATE TABLE IF NOT EXISTS`` os pularia em silêncio e a
    carga falharia adiante com erro de coluna.
    """
    for nome in (*TABELAS, *VIEWS):
        tipo = _relkind(conn, schema, nome)
        if tipo is None:
            continue
        esperado = "v" if nome in VIEWS else "r"
        colunas_ok = True
        if tipo == "r" and nome in _COLUNAS_CHAVE:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s",
                    (schema, nome),
                )
                colunas_ok = _COLUNAS_CHAVE[nome] <= {r[0] for r in cur.fetchall()}
        if tipo != esperado or not colunas_ok:
            raise EsquemaAntigoError(
                f"o schema {schema!r} já tem {nome!r} de outro esquema (tabela do "
                "sidra-sql legado ou de outra carga), que não é a do modelo novo do "
                "SIDRA. Mova esses objetos para outro schema (ou renomeie-os) e rode "
                "a carga de novo; o quantilica-sql não sobrescreve objetos alheios."
            )


def _versao_marcada(conn: psycopg.Connection, schema: str) -> bool:
    """O schema já tem o esquema desta versão (marca no ``COMMENT`` de ``tabela``)?"""
    if _relkind(conn, schema, "tabela") != "r":
        return False
    with conn.cursor() as cur:
        cur.execute(
            "SELECT obj_description(c.oid, 'pg_class') FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = 'tabela'",
            (schema,),
        )
        row = cur.fetchone()
    return row is not None and row[0] == _MARCA


def criar_esquema(
    conn: psycopg.Connection, schema: str, *, forcar: bool = False
) -> None:
    """Cria o esquema do SIDRA (idempotente) e o ledger ``arquivo_carregado``.

    Só ``CREATE SCHEMA`` quando o schema falta; as demais DDL pedem ``CREATE`` no
    schema, nunca no banco. Um schema já criado nesta versão do esquema (marca no
    ``COMMENT`` de ``tabela``) não reexecuta a DDL, o que também permite a um papel
    sem posse das views carregar; ``forcar`` reexecuta. Dentro de
    ``atomic_file_load`` não commita.
    """
    if esquema_antigo(conn, schema):
        raise EsquemaAntigoError(mensagem_esquema_antigo(schema))
    ensure_schema(conn, schema)
    ensure_tracker_table(conn, schema)
    if not forcar and _versao_marcada(conn, schema):
        return
    _conferir_objetos_alheios(conn, schema)
    ident = sql.Identifier(schema)
    with conn.cursor() as cur:
        cur.execute(sql.SQL(_DDL_TABELAS).format(s=ident))
        cur.execute(sql.SQL(_DDL_VIEWS).format(s=ident))
        cur.execute(
            sql.SQL("COMMENT ON TABLE {} IS {}").format(
                sql.Identifier(schema, "tabela"), sql.Literal(_MARCA)
            )
        )
    commit_if_standalone(conn)


def preparar_esquema(
    conn: psycopg.Connection, schema: str, *, recriar: bool = False
) -> None:
    """Garante o esquema novo, tratando o antigo.

    Raises:
        EsquemaAntigoError: o ``dados`` é do esquema antigo e ``recriar`` é falso.
    """
    if esquema_antigo(conn, schema):
        if not recriar:
            raise EsquemaAntigoError(mensagem_esquema_antigo(schema))
        recriar_esquema(conn, schema)
        return
    criar_esquema(conn, schema)


def recriar_esquema(conn: psycopg.Connection, schema: str) -> None:
    """Renomeia o ``dados`` antigo, apaga o registro de cargas do SIDRA e recria.

    Raises:
        EsquemaAntigoError: já existe ``dados_legado_v0`` (uma recriação anterior
            não foi limpa): o dado antigo não é sobrescrito.
    """
    if _relkind(conn, schema, TABELA_ANTIGA) is not None:
        raise EsquemaAntigoError(
            f"já existe {schema}.{TABELA_ANTIGA} (de uma recriação anterior); apague "
            "ou renomeie essa tabela antes de recriar."
        )
    ident = sql.Identifier(schema)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("ALTER TABLE {s}.dados RENAME TO {antiga}").format(
                s=ident, antiga=sql.Identifier(TABELA_ANTIGA)
            )
        )
    ensure_tracker_table(conn, schema)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DELETE FROM {} WHERE dataset = %s").format(
                sql.Identifier(schema, "arquivo_carregado")
            ),
            (DATASET,),
        )
    logger.warning(
        "SIDRA: %s.dados (esquema antigo) renomeada para %s.%s; esquema novo criado.",
        schema,
        schema,
        TABELA_ANTIGA,
    )
    criar_esquema(conn, schema)


def resumo(conn: psycopg.Connection, schema: str) -> dict[str, Any]:
    """Contagens do esquema: tabelas, versões, observações e ativas.

    Zeros quando o esquema novo ainda não existe.
    """
    zeros: dict[str, Any] = {
        "tabelas": 0,
        "versoes": 0,
        "observacoes": 0,
        "observacoes_ativas": 0,
    }
    if _relkind(conn, schema, "observacao") != "r":
        return zeros
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT (SELECT count(*) FROM {s}.tabela), "
                "(SELECT count(*) FROM {s}.versao), "
                "count(*), count(*) FILTER (WHERE ativo) FROM {s}.observacao"
            ).format(s=sql.Identifier(schema))
        )
        t, v, o, a = cur.fetchone() or (0, 0, 0, 0)
    return {
        "tabelas": t,
        "versoes": v,
        "observacoes": o,
        "observacoes_ativas": a,
    }
