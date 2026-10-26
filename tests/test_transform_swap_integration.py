"""Integração com PostgreSQL real: troca atômica do ``replace`` e views dependentes.

Exige ``QUANTILICA_TEST_DATABASE_URL``; sem ela, os testes são pulados.
"""

from __future__ import annotations

import os
import threading
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

from quantilica_sql.transform_runner import TransformRunner

URL = os.environ.get("QUANTILICA_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="QUANTILICA_TEST_DATABASE_URL ausente")


@pytest.fixture
def schema():
    nome = f"qsql_s_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(URL, autocommit=True) as conn:
        yield nome
        conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(nome))
        )


def _transform(tmp_path: Path, schema: str, query: str, extra: str = "") -> Path:
    (tmp_path / "q.sql").write_text(query, encoding="utf-8")
    toml = tmp_path / "transform.toml"
    toml.write_text(
        f'[[table]]\nname="t"\nschema="{schema}"\nstrategy="replace"\nsql="q.sql"\n'
        'primary_key=["a"]\nindexes=[{name="ix_b", columns=["b"]}]\n' + extra,
        encoding="utf-8",
    )
    return toml


def _run(schema: str, toml: Path) -> None:
    with psycopg.connect(URL) as conn:
        TransformRunner().run_transform_toml(conn, toml)


def _rows(schema: str, relation: str = "t") -> list[tuple]:
    with psycopg.connect(URL) as conn:
        return conn.execute(
            sql.SQL("SELECT * FROM {} ORDER BY 1").format(
                sql.Identifier(schema, relation)
            )
        ).fetchall()


def _nomes(schema: str) -> set[str]:
    with psycopg.connect(URL) as conn:
        return {
            r[0]
            for r in conn.execute(
                "SELECT relname FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = %s",
                (schema,),
            )
        }


def test_troca_preserva_nomes_de_pk_e_indices_e_nao_deixa_residuos(tmp_path, schema):
    toml = _transform(tmp_path, schema, "SELECT 1 AS a, 'x' AS b")
    _run(schema, toml)
    (tmp_path / "q.sql").write_text("SELECT 2 AS a, 'y' AS b UNION ALL SELECT 3, 'z'")
    _run(schema, toml)
    assert _rows(schema) == [(2, "y"), (3, "z")]
    assert _nomes(schema) == {"t", "t_pkey", "ix_b"}


def test_falha_na_reconstrucao_mantem_a_tabela_anterior(tmp_path, schema):
    toml = _transform(tmp_path, schema, "SELECT 1 AS a, 'x' AS b")
    _run(schema, toml)
    (tmp_path / "q.sql").write_text("SELECT 1 AS a, 'x' AS b UNION ALL SELECT 1, 'y'")
    with pytest.raises(psycopg.errors.UniqueViolation):  # PK duplicada
        _run(schema, toml)
    assert _rows(schema) == [(1, "x")]
    assert _nomes(schema) == {"t", "t_pkey", "ix_b"}


def test_views_dependentes_em_cadeia_sao_recriadas(tmp_path, schema):
    toml = _transform(tmp_path, schema, "SELECT 1 AS a, 'x' AS b")
    _run(schema, toml)
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute(
            sql.SQL("CREATE VIEW {} AS SELECT a, b FROM {}").format(
                sql.Identifier(schema, "v1"), sql.Identifier(schema, "t")
            )
        )
        # v2 depende de v1 e também direto da tabela
        conn.execute(
            sql.SQL(
                "CREATE VIEW {} AS SELECT v1.a, t.b FROM {} v1 JOIN {} t USING (a)"
            ).format(
                sql.Identifier(schema, "v2"),
                sql.Identifier(schema, "v1"),
                sql.Identifier(schema, "t"),
            )
        )
    (tmp_path / "q.sql").write_text("SELECT 5 AS a, 'n' AS b")
    _run(schema, toml)
    assert _rows(schema, "v1") == [(5, "n")]
    assert _rows(schema, "v2") == [(5, "n")]


def test_materialized_view_dependente_falha_sem_perder_dados(tmp_path, schema):
    toml = _transform(tmp_path, schema, "SELECT 1 AS a, 'x' AS b")
    _run(schema, toml)
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute(
            sql.SQL("CREATE MATERIALIZED VIEW {} AS SELECT a FROM {}").format(
                sql.Identifier(schema, "mv"), sql.Identifier(schema, "t")
            )
        )
    (tmp_path / "q.sql").write_text("SELECT 9 AS a, 'q' AS b")
    with pytest.raises(ValueError, match="materialized view"):
        _run(schema, toml)
    assert _rows(schema) == [(1, "x")]


def test_leitor_nao_e_bloqueado_enquanto_a_tabela_nova_e_construida(tmp_path, schema):
    """A construção (pg_sleep na consulta) não segura lock no destino."""
    toml = _transform(tmp_path, schema, "SELECT 1 AS a, 'x' AS b")
    _run(schema, toml)
    (tmp_path / "q.sql").write_text(
        "SELECT 2 AS a, 'y' AS b FROM (SELECT pg_sleep(1.5)) s"
    )
    construtor = threading.Thread(target=_run, args=(schema, toml))
    construtor.start()
    try:
        with psycopg.connect(URL, autocommit=True) as leitor:
            leitor.execute("SET lock_timeout = '300ms'")
            leitor.execute("SELECT pg_sleep(0.3)")  # deixa o construtor entrar no CTAS
            assert (
                leitor.execute(
                    sql.SQL("SELECT count(*) FROM {}").format(
                        sql.Identifier(schema, "t")
                    )
                ).fetchone()[0]
                == 1
            )
    finally:
        construtor.join()
    assert _rows(schema) == [(2, "y")]


def test_view_com_dependente_falha_em_vez_de_derrubar_em_cascata(tmp_path, schema):
    (tmp_path / "q.sql").write_text("SELECT 1 AS a")
    toml = tmp_path / "transform.toml"
    toml.write_text(
        f'[[table]]\nname="v"\nschema="{schema}"\nstrategy="view"\nsql="q.sql"\n',
        encoding="utf-8",
    )
    _run(schema, toml)
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute(
            sql.SQL("CREATE VIEW {} AS SELECT a FROM {}").format(
                sql.Identifier(schema, "filha"), sql.Identifier(schema, "v")
            )
        )
    with pytest.raises(psycopg.errors.DependentObjectsStillExist):
        _run(schema, toml)
    assert _rows(schema, "filha") == [(1,)]
