"""Testes do validador (pipelines reais) e do runner de transform.toml (mock)."""

from __future__ import annotations

import contextlib
from pathlib import Path

import pytest

from quantilica_sql import runner as runner_mod
from quantilica_sql.runner import (
    find_pipeline,
    infer_raw_schema,
    iter_transform_dirs,
    run_pipeline,
)
from quantilica_sql.transform_runner import TransformRunner, load_table_specs
from quantilica_sql.validator import PipelineValidator, find_pipeline_dirs

CATALOG = Path(__file__).resolve().parents[2] / "quantilica-pipelines"
needs_catalog = pytest.mark.skipif(
    not (CATALOG / "manifest.toml").exists(), reason="quantilica-pipelines ausente"
)


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# Validador contra o catálogo real
# --------------------------------------------------------------------------


@needs_catalog
def test_real_catalog_validates_without_issues():
    report = PipelineValidator().validate_catalog(CATALOG)
    assert report.ok, "\n".join(map(str, report.errors))
    assert report.warnings == []


@needs_catalog
def test_real_catalog_covers_all_pipelines():
    dirs = find_pipeline_dirs(CATALOG)
    assert len(dirs) >= 33
    v = PipelineValidator()
    for d in dirs:
        assert v.validate_pipeline_dir(d).ok, d


@needs_catalog
def test_real_cross_domain_fetch_is_valid():
    path = CATALOG / "cross" / "macro_trabalho" / "fetch.toml"
    assert PipelineValidator().validate_fetch(path).ok


# --------------------------------------------------------------------------
# Validador — casos sintéticos
# --------------------------------------------------------------------------


def test_manifest_legacy_array_format(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "fetch.toml").write_text("[[tabelas]]\ntabela_sidra='1'\n")
    m = write(
        tmp_path / "manifest.toml",
        'name="x"\ndescription="d"\nversion="1"\n[[pipeline]]\nid="a"\npath="a"\n',
    )
    assert PipelineValidator().validate_manifest(m).ok


def test_manifest_errors(tmp_path):
    m = write(
        tmp_path / "manifest.toml",
        '[catalog]\nname="x"\n[pipelines.p]\npath="nope"\n[pipelines.q]\ntheme="t"\n',
    )
    msgs = " ".join(i.message for i in PipelineValidator().validate_manifest(m).errors)
    assert "não existe" in msgs
    assert "'path' ausente" in msgs


def test_manifest_without_pipelines(tmp_path):
    m = write(tmp_path / "manifest.toml", "[catalog]\nname='x'\n")
    assert not PipelineValidator().validate_manifest(m).ok


def test_fetch_variants(tmp_path):
    v = PipelineValidator()
    ok_series = write(tmp_path / "s.toml", "[[series]]\nids=[1,2]\n")
    ok_pipe = write(tmp_path / "p.toml", '[pipeline]\nname="x"\ndomains=["a"]\n')
    assert v.validate_fetch(ok_series).ok
    assert v.validate_fetch(ok_pipe).ok
    assert not v.validate_fetch(write(tmp_path / "e.toml", "x=1\n")).ok
    assert not v.validate_fetch(write(tmp_path / "t.toml", "[[tabelas]]\nfoo=1\n")).ok
    assert not v.validate_fetch(
        write(tmp_path / "i.toml", "[[series]]\nids=['a']\n")
    ).ok
    assert not v.validate_fetch(write(tmp_path / "b.toml", "[[series]]\nx=1\n")).ok


def test_transform_validation_errors(tmp_path):
    write(tmp_path / "ok.sql", "select 1")
    t = write(
        tmp_path / "transform.toml",
        """
[[table]]
name="a"
schema="analytics"
strategy="table"
sql="ok.sql"

[[table]]
name="b"
schema="analytics"
strategy="replace"
sql="missing.sql"
primary_key="id"

[[table]]
name="b"
schema="analytics"
strategy="replace"
sql="ok.sql"
indexes=[{name="i", columns=[]}, {columns=["c"]}]
""",
    )
    msgs = " | ".join(
        i.message for i in PipelineValidator().validate_transform(t).errors
    )
    for expected in (
        "strategy 'table' inválida",
        "'missing.sql' não existe",
        "'primary_key' deve ser lista",
        "duplicado",
        "'columns' deve ser lista",
        "'name' ausente",
    ):
        assert expected in msgs


def test_transform_singular_table_rejected(tmp_path):
    t = write(tmp_path / "transform.toml", "[table]\nname='a'\n")
    assert not PipelineValidator().validate_transform(t).ok


def test_transform_view_with_pk_warns(tmp_path):
    write(tmp_path / "v.sql", "select 1")
    t = write(
        tmp_path / "transform.toml",
        '[[table]]\nname="v"\nschema="s"\nstrategy="view"\nsql="v.sql"\n'
        'primary_key=["a"]\n',
    )
    report = PipelineValidator().validate_transform(t)
    assert report.ok and len(report.warnings) == 1


def test_invalid_toml_reported(tmp_path):
    t = write(tmp_path / "transform.toml", "[[table\n")
    report = PipelineValidator().validate_transform(t)
    assert not report.ok and "TOML inválido" in report.errors[0].message


# --------------------------------------------------------------------------
# TransformRunner com conexão fake
# --------------------------------------------------------------------------


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, query, params=None):
        text = query if isinstance(query, str) else query.as_string()
        if self.conn.fail_on and self.conn.fail_on in text:
            raise RuntimeError("boom")
        self.conn.log.append(text)

    def fetchone(self):
        return (None,)  # to_regclass: o destino ainda não existe

    def fetchall(self):
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self, fail_on: str | None = None):
        self.log: list[str] = []
        self.fail_on = fail_on

    @contextlib.contextmanager
    def transaction(self):
        self.log.append("BEGIN")
        try:
            yield
        except BaseException:
            self.log.append("ROLLBACK")
            raise
        self.log.append("COMMIT")

    def cursor(self):
        return FakeCursor(self)


def make_pipeline(
    tmp_path: Path, toml_body: str, sql_text="select 1 as a, '100%' as p"
):
    write(tmp_path / "q.sql", sql_text)
    return write(tmp_path / "transform.toml", toml_body)


REPLACE = """
[[table]]
name="t"
schema="analytics"
strategy="replace"
sql="q.sql"
primary_key=["a","b"]
indexes=[{name="ix_a", columns=["a"], unique=true}, {name="ix_b", columns=["b"]}]
"""


def test_replace_sequence(tmp_path):
    toml = make_pipeline(tmp_path, REPLACE)
    conn = FakeConn()
    done = TransformRunner("ibge_sidra").run_transform_toml(conn, toml)
    assert done == ["analytics.t"]
    assert conn.log == [
        "BEGIN",
        'CREATE SCHEMA IF NOT EXISTS "analytics"',
        'SET LOCAL search_path TO "ibge_sidra", public',
        'DROP TABLE IF EXISTS "analytics"."t__new"',
        'CREATE TABLE "analytics"."t__new" AS select 1 as a, \'100%\' as p',
        'ALTER TABLE "analytics"."t__new" ADD PRIMARY KEY ("a", "b")',
        'CREATE UNIQUE INDEX "ix_a__new" ON "analytics"."t__new" ("a")',
        'CREATE INDEX "ix_b__new" ON "analytics"."t__new" ("b")',
        "SELECT relkind FROM pg_class WHERE oid = to_regclass(%s)",
        'ALTER TABLE "analytics"."t__new" RENAME TO "t"',
        'ALTER INDEX "analytics"."t__new_pkey" RENAME TO "t_pkey"',
        'ALTER INDEX "analytics"."ix_a__new" RENAME TO "ix_a"',
        'ALTER INDEX "analytics"."ix_b__new" RENAME TO "ix_b"',
        "COMMIT",
    ]


def test_view_sequence_and_no_search_path_when_unset(tmp_path):
    toml = make_pipeline(
        tmp_path,
        '[[table]]\nname="v"\nschema="analytics"\nstrategy="view"\nsql="q.sql"\n'
        'primary_key=["a"]\n',
        sql_text="select 1;\n",
    )
    conn = FakeConn()
    TransformRunner().run_transform_toml(conn, toml)
    assert conn.log == [
        "BEGIN",
        'CREATE SCHEMA IF NOT EXISTS "analytics"',
        'DROP VIEW IF EXISTS "analytics"."v"',
        'CREATE VIEW "analytics"."v" AS select 1',
        "COMMIT",
    ]


def test_identifiers_are_escaped(tmp_path):
    toml = make_pipeline(
        tmp_path,
        '[[table]]\nname=\'a"b\'\nschema="analytics"\nstrategy="replace"\nsql="q.sql"\n',
    )
    conn = FakeConn()
    TransformRunner().run_transform_toml(conn, toml)
    assert 'DROP TABLE IF EXISTS "analytics"."a""b__new"' in conn.log


def test_each_table_has_own_transaction_and_failure_isolated(tmp_path):
    body = (
        '[[table]]\nname="one"\nschema="s"\nstrategy="replace"\nsql="q.sql"\n'
        '[[table]]\nname="two"\nschema="s"\nstrategy="replace"\nsql="q.sql"\n'
        '[[table]]\nname="three"\nschema="s"\nstrategy="replace"\nsql="q.sql"\n'
    )
    toml = make_pipeline(tmp_path, body)
    conn = FakeConn(fail_on='CREATE TABLE "s"."two__new"')
    with pytest.raises(RuntimeError, match="boom"):
        TransformRunner().run_transform_toml(conn, toml)
    assert conn.log.count("BEGIN") == 2
    assert conn.log.count("COMMIT") == 1
    assert conn.log.count("ROLLBACK") == 1
    assert not any('"three"' in line for line in conn.log)


def test_invalid_transform_raises_before_touching_db(tmp_path):
    toml = make_pipeline(
        tmp_path,
        '[[table]]\nname="t"\nschema="s"\nstrategy="pivot"\nsql="q.sql"\n',
    )
    conn = FakeConn()
    with pytest.raises(ValueError, match="inválida"):
        TransformRunner().run_transform_toml(conn, toml)
    assert conn.log == []


def test_empty_sql_rejected(tmp_path):
    toml = make_pipeline(tmp_path, REPLACE, sql_text=" ;\n")
    with pytest.raises(ValueError, match="vazio"):
        TransformRunner().run_transform_toml(FakeConn(), toml)


@needs_catalog
def test_load_specs_for_all_real_transforms():
    total = 0
    for d in find_pipeline_dirs(CATALOG):
        if (d / "transform.toml").exists():
            specs = load_table_specs(d / "transform.toml")
            assert specs and all(s.sql_path.is_file() for s in specs)
            total += len(specs)
    assert total >= 69


# --------------------------------------------------------------------------
# Runner (descoberta, ordem, run_pipeline)
# --------------------------------------------------------------------------


def test_iter_transform_dirs_children_first(tmp_path):
    write(tmp_path / "transform.toml", "")
    write(tmp_path / "b" / "transform.toml", "")
    write(tmp_path / "a" / "transform.toml", "")
    write(tmp_path / "a" / "deep" / "transform.toml", "")
    write(tmp_path / "empty" / "readme.md", "")
    assert iter_transform_dirs(tmp_path) == [
        tmp_path / "a" / "deep",
        tmp_path / "a",
        tmp_path / "b",
        tmp_path,
    ]


def test_infer_raw_schema(tmp_path):
    assert infer_raw_schema(tmp_path) is None
    write(tmp_path / "fetch.toml", "[[tabelas]]\ntabela_sidra='1'\n")
    assert infer_raw_schema(tmp_path) == "ibge_sidra"
    write(tmp_path / "fetch.toml", "[[series]]\nids=[1]\n")
    assert infer_raw_schema(tmp_path) == "bcb_sgs"
    write(tmp_path / "fetch.toml", '[pipeline]\nname="x"\n')
    assert infer_raw_schema(tmp_path) is None


def test_find_pipeline_by_manifest_id_and_path(tmp_path, monkeypatch):
    write(tmp_path / "cat" / "m" / "p" / "transform.toml", "")
    write(
        tmp_path / "cat" / "manifest.toml",
        '[catalog]\nname="c"\n[pipelines.my_id]\npath="m/p"\n',
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(runner_mod, "WORKSPACE_CATALOG", "cat")
    monkeypatch.setattr(
        runner_mod,
        "PluginManager",
        lambda: type("M", (), {"list_plugins": lambda s: []})(),
    )
    assert find_pipeline("my_id") == Path("cat/m/p")
    assert find_pipeline("m/p") == Path("cat/m/p")
    assert find_pipeline("unknown") is None


def test_run_pipeline_uses_runner_per_dir(tmp_path, monkeypatch):
    write(tmp_path / "q.sql", "select 1")
    write(tmp_path / "fetch.toml", "[[series]]\nids=[1]\n")
    write(
        tmp_path / "transform.toml",
        '[[table]]\nname="t"\nschema="analytics"\nstrategy="replace"\nsql="q.sql"\n',
    )
    conn = FakeConn()

    class Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(runner_mod, "get_connection", lambda: Ctx())
    res = run_pipeline(str(tmp_path))
    assert res["status"] == "success"
    assert res["materialized"] == ["analytics.t"]
    assert 'SET LOCAL search_path TO "bcb_sgs", public' in conn.log


def test_run_pipeline_not_found(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        runner_mod,
        "PluginManager",
        lambda: type("M", (), {"list_plugins": lambda s: []})(),
    )
    with pytest.raises(FileNotFoundError):
        run_pipeline("nada/aqui")


# --------------------------------------------------------------------------
# Manifest: completude e engine
# --------------------------------------------------------------------------


def _catalogo(tmp_path: Path, manifest: str) -> Path:
    write(tmp_path / "a" / "um" / "transform.toml", TRANSFORM_OK)
    write(tmp_path / "a" / "um" / "t.sql", "SELECT 1")
    write(tmp_path / "b" / "dois" / "transform.toml", TRANSFORM_OK)
    write(tmp_path / "b" / "dois" / "t.sql", "SELECT 1")
    write(tmp_path / "manifest.toml", manifest)
    return tmp_path


TRANSFORM_OK = """
[[table]]
name = "t"
schema = "analytics"
strategy = "replace"
sql = "t.sql"
"""

MANIFEST_BASE = """
[catalog]
name = "x"

[pipelines.a]
path = "a"
{extra}
"""


def test_catalogo_com_pipeline_fora_do_manifest_e_erro(tmp_path):
    root = _catalogo(tmp_path, MANIFEST_BASE.format(extra=""))
    report = PipelineValidator().validate_catalog(root)
    mensagens = [i.message for i in report.errors]
    assert any("b/dois" in m and "não está coberto" in m for m in mensagens)
    assert not any("a/um" in m and "não está coberto" in m for m in mensagens)


def test_path_do_manifest_cobre_os_descendentes(tmp_path):
    manifesto = MANIFEST_BASE.format(extra="") + '\n[pipelines.b]\npath = "b"\n'
    report = PipelineValidator().validate_catalog(_catalogo(tmp_path, manifesto))
    assert report.ok, [str(i) for i in report.errors]


def test_engine_invalido_no_manifest(tmp_path):
    manifesto = MANIFEST_BASE.format(extra='engine = "spark"') + (
        '\n[pipelines.b]\npath = "b"\n'
    )
    report = PipelineValidator().validate_catalog(_catalogo(tmp_path, manifesto))
    assert any("engine 'spark' inválido" in i.message for i in report.errors)


def test_engines_validos(tmp_path):
    manifesto = (
        MANIFEST_BASE.format(extra='engine = "postgres"')
        + '\n[pipelines.b]\npath = "b"\nengine = "duckdb"\n'
    )
    assert PipelineValidator().validate_catalog(_catalogo(tmp_path, manifesto)).ok
