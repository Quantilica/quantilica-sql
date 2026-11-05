"""Validação das referências de tabelas/views dentro do SQL dos pipelines."""

from __future__ import annotations

from pathlib import Path

from quantilica_sql.validator import PipelineValidator, extract_table_refs

ALIASES = """
[sources.b3]
aliases = ["cotahist"]
[sources.rfb_cnpj]
aliases = ["empresas"]
legacy_aliases = ["empresas_antigas"]
"""


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def make_catalog(tmp_path: Path, engine: str, sql: str, aliases: bool = True) -> Path:
    write(
        tmp_path / "manifest.toml",
        f'[catalog]\nname="c"\n[pipelines.p]\npath="p"\nengine="{engine}"\n',
    )
    write(
        tmp_path / "p" / "transform.toml",
        '[[table]]\nname="t"\nschema="marts"\nstrategy="replace"\n'
        f'engine="{engine}"\nsql="t.sql"\n',
    )
    write(tmp_path / "p" / "t.sql", sql)
    if aliases:
        write(tmp_path / "tests" / "lake_aliases.toml", ALIASES)
    return tmp_path


def test_extract_table_refs_ignores_comments_strings_and_functions():
    sql = """
    -- FROM b3.fake
    SELECT 'FROM x.y' AS s, count(*) FROM b3.cotahist c
    JOIN rfb_cnpj.empresas USING (k) /* JOIN z.w */
    CROSS JOIN unnest(a.b)
    """
    assert extract_table_refs(sql) == {"b3.cotahist", "rfb_cnpj.empresas"}


def test_duckdb_known_lake_view_passes(tmp_path):
    root = make_catalog(tmp_path, "duckdb", "SELECT a FROM b3.cotahist")
    report = PipelineValidator().validate_catalog(root)
    assert report.ok, [str(i) for i in report.issues]


def test_duckdb_legacy_alias_passes(tmp_path):
    root = make_catalog(tmp_path, "duckdb", "SELECT a FROM rfb_cnpj.empresas_antigas")
    assert PipelineValidator().validate_catalog(root).ok


def test_duckdb_unknown_view_is_error(tmp_path):
    root = make_catalog(
        tmp_path,
        "duckdb",
        "SELECT a FROM b3.cotacaohistorica JOIN b3.cotahist USING(a)",
    )
    report = PipelineValidator().validate_catalog(root)
    msgs = [i.message for i in report.errors]
    assert any("b3.cotacaohistorica" in m for m in msgs)
    assert not any("b3.cotahist " in m + " " for m in msgs)


def test_duckdb_refs_not_checked_without_alias_file(tmp_path):
    root = make_catalog(tmp_path, "duckdb", "SELECT a FROM b3.nada", aliases=False)
    assert PipelineValidator().validate_catalog(root).ok


def test_postgres_refs_are_not_checked(tmp_path):
    # tabelas do engine postgres podem vir da carga bruta, invisível ao validador
    root = make_catalog(tmp_path, "postgres", "SELECT a FROM marts.fantasma")
    assert PipelineValidator().validate_catalog(root).ok
