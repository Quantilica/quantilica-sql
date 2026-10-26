"""Integração com PostgreSQL real: carga atômica, recarga por substituição e ledger.

Exige ``QUANTILICA_TEST_DATABASE_URL`` (banco descartável); sem ela, os testes são
pulados. Cada teste usa um schema próprio, removido ao final.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.core.atomic import LegacyLoadError, atomic_file_load
from quantilica_sql.core.copy_stream import copy_from_file, copy_from_rows

URL = os.environ.get("QUANTILICA_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not URL, reason="QUANTILICA_TEST_DATABASE_URL ausente")


class _Adapter(BaseSourceAdapter):
    def __init__(self, schema: str) -> None:
        self._schema = schema

    @property
    def domain(self) -> str:
        return "fonte"

    @property
    def schema(self) -> str:
        return self._schema

    def load(self, conn: Any, source_path: Path | None = None, **kwargs: Any) -> int:
        return 0

    def status(self, conn: Any) -> dict[str, Any]:
        return {}


@pytest.fixture(params=[False, True], ids=["tx", "autocommit"])
def env(request, tmp_path):
    schema = f"qsql_t_{uuid.uuid4().hex[:8]}"
    conn = psycopg.connect(URL, autocommit=request.param)
    adapter = _Adapter(schema)
    adapter.ensure_table_with_schema_evolution(conn, "dados", ["a", "b"])
    yield adapter, conn, tmp_path
    if not conn.autocommit:
        conn.rollback()
    conn.autocommit = True
    conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
    conn.close()


def _csv(path: Path, rows: list[tuple[str, str]]) -> Path:
    path.write_text("".join(f"{a};{b}\n" for a, b in rows), encoding="utf-8")
    return path


def _load(adapter, conn, path: Path, force: bool = False) -> int | None:
    ok, digest = adapter.should_load_file(conn, path, dataset="d", force=force)
    if not ok:
        return None
    with adapter.atomic_file(
        conn, table="dados", filename=path.name, dataset="d", sha256=digest
    ):
        n = copy_from_file(
            conn, adapter.schema, "dados", path, columns=["a", "b"], delimiter=";"
        )
        adapter.record_and_commit(conn, path.name, dataset="d", rows_loaded=n)
    return n


def _count(conn, adapter, where: str = "TRUE") -> int:
    q = sql.SQL("SELECT count(*) FROM {} WHERE " + where).format(
        sql.Identifier(adapter.schema, "dados")
    )
    return conn.execute(q).fetchone()[0]


def _ledger(conn, adapter):
    q = sql.SQL("SELECT nome_arquivo, linhas, sha256 FROM {} ORDER BY 1").format(
        sql.Identifier(adapter.schema, "arquivo_carregado")
    )
    return conn.execute(q).fetchall()


def test_carga_marca_arquivo_e_grava_sha(env):
    adapter, conn, tmp = env
    f = _csv(tmp / "x.csv", [("1", "a"), ("2", "b"), ("3", "c")])
    assert _load(adapter, conn, f) == 3
    assert _count(conn, adapter, "_arquivo = 'x.csv'") == 3
    ((nome, linhas, sha),) = _ledger(conn, adapter)
    assert (nome, linhas) == ("x.csv", 3) and len(sha) == 64


def test_mesmo_hash_nao_recarrega(env):
    adapter, conn, tmp = env
    f = _csv(tmp / "x.csv", [("1", "a")])
    assert _load(adapter, conn, f) == 1
    assert _load(adapter, conn, f) is None
    assert _count(conn, adapter) == 1


def test_republicado_com_outro_hash_substitui_sem_duplicar(env):
    adapter, conn, tmp = env
    f = _csv(tmp / "x.csv", [("1", "a"), ("2", "b")])
    _load(adapter, conn, f)
    outro = _csv(tmp / "y.csv", [("9", "z")])
    _load(adapter, conn, outro)
    _csv(f, [("1", "a"), ("2", "b"), ("3", "c"), ("4", "d")])
    assert _load(adapter, conn, f) == 4
    assert _count(conn, adapter, "_arquivo = 'x.csv'") == 4
    assert _count(conn, adapter, "_arquivo = 'y.csv'") == 1
    por_arquivo = {nome: linhas for nome, linhas, _ in _ledger(conn, adapter)}
    assert por_arquivo == {"x.csv": 4, "y.csv": 1}


def test_force_e_idempotente(env):
    adapter, conn, tmp = env
    f = _csv(tmp / "x.csv", [("1", "a"), ("2", "b")])
    _load(adapter, conn, f)
    _load(adapter, conn, f, force=True)
    _load(adapter, conn, f, force=True)
    assert _count(conn, adapter) == 2


def test_falha_apos_copy_nao_deixa_linhas_nem_ledger(env):
    adapter, conn, tmp = env
    f = _csv(tmp / "x.csv", [("1", "a"), ("2", "b")])
    ok, digest = adapter.should_load_file(conn, f, dataset="d")
    with pytest.raises(RuntimeError, match="queda"):
        with adapter.atomic_file(
            conn, table="dados", filename=f.name, dataset="d", sha256=digest
        ):
            copy_from_file(
                conn, adapter.schema, "dados", f, columns=["a", "b"], delimiter=";"
            )
            raise RuntimeError("queda entre o COPY e o ledger")
    assert _count(conn, adapter) == 0
    assert _ledger(conn, adapter) == []


def test_falha_na_recarga_preserva_a_carga_anterior(env):
    adapter, conn, tmp = env
    f = _csv(tmp / "x.csv", [("1", "a"), ("2", "b")])
    _load(adapter, conn, f)
    with pytest.raises(RuntimeError):
        with atomic_file_load(
            conn, schema=adapter.schema, table="dados", filename="x.csv", dataset="d"
        ):
            copy_from_rows(conn, adapter.schema, "dados", ["a", "b"], [("7", "q")])
            raise RuntimeError("falha")
    assert _count(conn, adapter, "_arquivo = 'x.csv'") == 2


def test_legado_sem_hash_ganha_hash_e_nao_recarrega(env):
    adapter, conn, tmp = env
    f = _csv(tmp / "x.csv", [("1", "a")])
    copy_from_file(conn, adapter.schema, "dados", f, columns=["a", "b"], delimiter=";")
    adapter.record_and_commit(conn, "x.csv", dataset="d", rows_loaded=1)
    assert _ledger(conn, adapter)[0][2] is None
    assert _load(adapter, conn, f) is None
    assert _ledger(conn, adapter)[0][2] is not None
    assert _count(conn, adapter) == 1


def test_force_sobre_carga_legada_falha_sem_duplicar(env):
    adapter, conn, tmp = env
    f = _csv(tmp / "x.csv", [("1", "a")])
    copy_from_file(conn, adapter.schema, "dados", f, columns=["a", "b"], delimiter=";")
    adapter.record_and_commit(conn, "x.csv", dataset="d", rows_loaded=1)
    with pytest.raises(LegacyLoadError):
        _load(adapter, conn, f, force=True)
    assert _count(conn, adapter) == 1


# --- adapters reais, ponta a ponta -------------------------------------------


@pytest.fixture
def real_schema():
    conn = psycopg.connect(URL, autocommit=False)
    created: list[str] = []

    def use(adapter) -> psycopg.Connection:
        # isola cada teste num schema próprio sem tocar o código do adapter
        schema = f"qsql_t_{uuid.uuid4().hex[:8]}"
        type(adapter).schema = property(lambda self, s=schema: s)
        created.append(schema)
        return conn

    yield use
    conn.rollback()
    conn.autocommit = True
    for schema in created:
        conn.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
        )
    conn.close()


def test_anp_republicacao_substitui_sem_duplicar(real_schema, tmp_path):
    from quantilica_sql.adapters.anp import AnpAdapter

    adapter = AnpAdapter()
    original = type(adapter).schema
    try:
        conn = real_schema(adapter)
        pasta = tmp_path / "anp" / "g"
        pasta.mkdir(parents=True)
        arq = pasta / "precos.csv"
        arq.write_text("Estado;Preco\nSP;1,0\nRJ;2,0\n", encoding="utf-8")

        assert adapter.load(conn, source_path=tmp_path / "anp") == 2
        assert adapter.load(conn, source_path=tmp_path / "anp") == 0  # mesmo hash

        arq.write_text("Estado;Preco\nSP;1,0\nRJ;2,0\nMG;3,0\n", encoding="utf-8")
        assert adapter.load(conn, source_path=tmp_path / "anp") == 3

        tabela = sql.Identifier(adapter.schema, "g_precos")
        total = conn.execute(
            sql.SQL("SELECT count(*) FROM {}").format(tabela)
        ).fetchone()
        assert total[0] == 3
        fontes = conn.execute(
            sql.SQL("SELECT DISTINCT _arquivo FROM {}").format(tabela)
        ).fetchall()
        assert fontes == [("precos.csv",)]
    finally:
        type(adapter).schema = original


def test_cvm_zip_multitabela_substitui_todas_as_tabelas(real_schema, tmp_path):
    import zipfile

    from quantilica_sql.adapters.cvm import CvmAdapter

    adapter = CvmAdapter()
    original = type(adapter).schema
    try:
        conn = real_schema(adapter)
        pasta = tmp_path / "cvm" / "grupo"
        pasta.mkdir(parents=True)
        zpath = pasta / "pacote.zip"

        def escreve(a_linhas: list[str], b_linhas: list[str]) -> None:
            with zipfile.ZipFile(zpath, "w") as z:
                z.writestr("tabela_a.csv", "COL;VAL\n" + "\n".join(a_linhas) + "\n")
                z.writestr("tabela_b.csv", "COL;VAL\n" + "\n".join(b_linhas) + "\n")

        escreve(["a;1", "b;2"], ["x;9"])
        assert adapter.load(conn, source_path=tmp_path / "cvm") == 3
        escreve(["a;1"], ["x;9", "y;8"])
        assert adapter.load(conn, source_path=tmp_path / "cvm") == 3

        contagens = {
            t: conn.execute(
                sql.SQL("SELECT count(*) FROM {}").format(
                    sql.Identifier(adapter.schema, t)
                )
            ).fetchone()[0]
            for t in ("tabela_a", "tabela_b")
        }
        assert contagens == {"tabela_a": 1, "tabela_b": 2}
        meta = conn.execute(
            sql.SQL("SELECT metadados FROM {}").format(
                sql.Identifier(adapter.schema, "arquivo_carregado")
            )
        ).fetchone()[0]
        assert meta["_tabelas"] == ["tabela_a", "tabela_b"]
    finally:
        type(adapter).schema = original
