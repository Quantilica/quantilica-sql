"""Engine duckdb ponta a ponta: lake de Parquet → consulta DuckDB → tabela no Postgres.

Exige ``QUANTILICA_TEST_DATABASE_URL`` e o extra ``lake`` (duckdb).
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

pytest.importorskip("duckdb")

from quantilica_sql.core.compare import compare_schemas  # noqa: E402
from quantilica_sql.lake.duck import connect_lake, pg_type  # noqa: E402
from quantilica_sql.transform_runner import TransformRunner  # noqa: E402
from quantilica_sql.validator import PipelineValidator  # noqa: E402

URL = os.environ.get("QUANTILICA_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="QUANTILICA_TEST_DATABASE_URL ausente")


@pytest.fixture
def schema():
    nome = f"qsql_l_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(URL, autocommit=True) as conn:
        yield nome
        conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(nome))
        )


@pytest.fixture
def lake(tmp_path, write_hive) -> Path:
    raiz = tmp_path / "lake"
    write_hive(
        raiz,
        "tesouro-direto",
        "investidores-do-tesouro-direto",
        "Codigo do Investidor;Data de Adesao;Genero;Idade;UF do Investidor\n"
        "1;01/02/2018;M;25;SP\n2;03/04/2018;F;41;RJ\n3;05/06/2018;F;;SP\n",
        key="investidores-do-tesouro-direto-de-2018",
        partition="2018",
        alias="investidores",
        # o conversor do Tesouro Direto renomeia a idade (publicações antigas)
        rename={"idade": "idade_do_investidor"},
    )
    return raiz


def _transform(tmp_path: Path, schema: str, query: str, extra: str = "") -> Path:
    (tmp_path / "q.sql").write_text(query, encoding="utf-8")
    toml = tmp_path / "transform.toml"
    toml.write_text(
        f'[[table]]\nname="t"\nschema="{schema}"\nstrategy="replace"\n'
        f'engine="duckdb"\nsql="q.sql"\n' + extra,
        encoding="utf-8",
    )
    return toml


def _run(toml: Path, lake: Path) -> None:
    with psycopg.connect(URL) as conn:
        TransformRunner(lake=lake).run_transform_toml(conn, toml)


def _rows(schema: str, relation: str = "t") -> list[tuple]:
    with psycopg.connect(URL) as conn:
        return conn.execute(
            sql.SQL("SELECT * FROM {} ORDER BY 1").format(
                sql.Identifier(schema, relation)
            )
        ).fetchall()


def test_views_do_lake_usam_apelido_e_expoem_o_arquivo_de_origem(lake):
    duck = connect_lake(lake)
    cols = [
        d[0]
        for d in duck.execute("SELECT * FROM tesouro_direto.investidores").description
    ]
    assert "uf_do_investidor" in cols and "_arquivo" in cols
    assert "periodo" in cols  # a partição Hive vira coluna
    assert duck.execute(
        "SELECT DISTINCT _arquivo, periodo FROM tesouro_direto.investidores"
    ).fetchall() == [("investidores-do-tesouro-direto-de-2018.parquet", "2018")]
    n = duck.execute("SELECT count(*) FROM tesouro_direto.investidores").fetchone()[0]
    assert n == 3


def test_consulta_duckdb_vira_tabela_tipada_no_postgres(tmp_path, lake, schema):
    toml = _transform(
        tmp_path,
        schema,
        """
        SELECT uf_do_investidor AS uf,
               count(*) AS n,
               sum(TRY_CAST(idade_do_investidor AS INTEGER)) AS soma_idade,
               min(strptime(data_de_adesao, '%d/%m/%Y')::DATE) AS primeira_adesao,
               CAST(1.5 AS DECIMAL(10,2)) AS taxa
        FROM tesouro_direto.investidores GROUP BY 1
        """,
        'primary_key=["uf"]\n',
    )
    _run(toml, lake)
    rows = _rows(schema)
    assert [r[0] for r in rows] == ["RJ", "SP"]
    assert rows[1][1:3] == (2, 25)
    assert str(rows[0][3]) == "2018-04-03"
    with psycopg.connect(URL) as conn:
        tipos = dict(
            conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name='t'",
                (schema,),
            ).fetchall()
        )
    assert tipos == {
        "uf": "text",
        "n": "bigint",
        "soma_idade": "numeric",  # sum() de INTEGER é HUGEINT no DuckDB
        "primeira_adesao": "date",
        "taxa": "numeric",
    }


def test_null_e_texto_vazio_sao_distintos(tmp_path, lake, schema):
    toml = _transform(
        tmp_path,
        schema,
        "SELECT 1 AS id, NULL::VARCHAR AS a, '' AS b, 'x,y\"z' AS c",
    )
    _run(toml, lake)
    assert _rows(schema) == [(1, None, "", 'x,y"z')]


def test_prelude_e_compartilhado_entre_as_tabelas(tmp_path, lake, schema):
    (tmp_path / "prelude.sql").write_text(
        "CREATE VIEW investidor AS SELECT trim(uf_do_investidor) AS uf "
        "FROM tesouro_direto.investidores;"
    )
    (tmp_path / "a.sql").write_text(
        "SELECT uf, count(*) AS n FROM investidor GROUP BY 1"
    )
    (tmp_path / "b.sql").write_text("SELECT count(DISTINCT uf) AS ufs FROM investidor")
    toml = tmp_path / "transform.toml"
    toml.write_text(
        'prelude = "prelude.sql"\n'
        f'[[table]]\nname="a"\nschema="{schema}"\nstrategy="replace"\nengine="duckdb"\nsql="a.sql"\n'
        f'[[table]]\nname="b"\nschema="{schema}"\nstrategy="replace"\nengine="duckdb"\nsql="b.sql"\n'
    )
    assert PipelineValidator().validate_transform(toml).ok
    _run(toml, lake)
    assert _rows(schema, "a") == [("RJ", 1), ("SP", 2)]
    assert _rows(schema, "b") == [(2,)]


def test_troca_substitui_uma_materialized_view_existente(tmp_path, lake, schema):
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(
            sql.SQL("CREATE MATERIALIZED VIEW {} AS SELECT 'velho'::text AS uf").format(
                sql.Identifier(schema, "t")
            )
        )
    toml = _transform(
        tmp_path,
        schema,
        "SELECT DISTINCT uf_do_investidor AS uf FROM tesouro_direto.investidores",
    )
    _run(toml, lake)
    assert _rows(schema) == [("RJ",), ("SP",)]
    with psycopg.connect(URL) as conn:
        kind = conn.execute(
            "SELECT relkind FROM pg_class WHERE oid = to_regclass(%s)",
            (f'"{schema}"."t"',),
        ).fetchone()[0]
    assert kind == "r"  # agora é tabela


def test_falha_na_consulta_duckdb_preserva_a_tabela_anterior(tmp_path, lake, schema):
    toml = _transform(tmp_path, schema, "SELECT 1 AS a")
    _run(toml, lake)
    (tmp_path / "q.sql").write_text(
        "SELECT coluna_inexistente FROM tesouro_direto.investidores"
    )
    with pytest.raises(Exception, match="coluna_inexistente"):
        _run(toml, lake)
    assert _rows(schema) == [(1,)]


def test_compare_confere_duas_execucoes_sobre_o_mesmo_lake(tmp_path, lake, schema):
    outro = f"{schema}_b"
    q = (
        "SELECT uf_do_investidor AS uf, count(*) AS n "
        "FROM tesouro_direto.investidores GROUP BY 1"
    )
    try:
        _run(_transform(tmp_path, schema, q), lake)
        _run(_transform(tmp_path, outro, q), lake)
        with psycopg.connect(URL) as conn:
            report = compare_schemas(conn, schema, outro, ["t"])
        assert report.ok
        assert [d.rows_a for d in report.diffs] == [2]
    finally:
        with psycopg.connect(URL, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(outro)
                )
            )


def test_validador_rejeita_view_com_duckdb_e_engine_invalido(tmp_path):
    (tmp_path / "q.sql").write_text("SELECT 1")
    toml = tmp_path / "transform.toml"
    toml.write_text(
        '[[table]]\nname="a"\nschema="s"\nstrategy="view"\nengine="duckdb"\nsql="q.sql"\n'
        '[[table]]\nname="b"\nschema="s"\nstrategy="replace"\nengine="spark"\nsql="q.sql"\n'
    )
    msgs = [i.message for i in PipelineValidator().validate_transform(toml).errors]
    assert any("duckdb só aceita strategy replace" in m for m in msgs)
    assert any("engine 'spark' inválido" in m for m in msgs)


def test_mapeamento_de_tipos():
    assert pg_type("DECIMAL(18,2)") == "numeric(18,2)"
    assert pg_type("TIMESTAMP WITH TIME ZONE") == "timestamptz"
    assert pg_type("LIST(INTEGER)") == "text"
