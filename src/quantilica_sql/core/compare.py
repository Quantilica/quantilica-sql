"""Comparação tabela a tabela entre duas versões de um schema (gate de paridade)."""

from __future__ import annotations

from dataclasses import dataclass, field

import psycopg
from psycopg import sql


@dataclass
class TableDiff:
    table: str
    rows_a: int | None = None
    rows_b: int | None = None
    only_a: int = 0
    only_b: int = 0
    note: str = ""

    @property
    def ok(self) -> bool:
        return not self.note and not self.only_a and not self.only_b


@dataclass
class CompareReport:
    diffs: list[TableDiff] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(d.ok for d in self.diffs)


def _tables(conn: psycopg.Connection, schema: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = "
            "c.relnamespace WHERE n.nspname = %s AND c.relkind IN ('r', 'p', 'm', 'v')",
            (schema,),
        )
        return {r[0] for r in cur.fetchall()}


def _columns(conn: psycopg.Connection, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT a.attname FROM pg_attribute a WHERE a.attrelid = "
            "to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            (f'"{schema}"."{table}"',),
        )
        return [r[0] for r in cur.fetchall()]


def _count(conn: psycopg.Connection, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(schema, table))
        )
        return cur.fetchone()[0]


def _except_count(
    conn: psycopg.Connection, left: str, right: str, table: str, cols: list[str]
) -> int:
    select = sql.SQL(", ").join(sql.Identifier(c) for c in cols)
    query = sql.SQL(
        "SELECT count(*) FROM (SELECT {c} FROM {l} EXCEPT ALL SELECT {c} FROM {r}) x"
    ).format(
        c=select,
        l=sql.Identifier(left, table),
        r=sql.Identifier(right, table),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]


def compare_schemas(
    conn: psycopg.Connection,
    schema_a: str,
    schema_b: str,
    tables: list[str] | None = None,
) -> CompareReport:
    """Compara contagem e conteúdo (``EXCEPT ALL`` nos dois sentidos) das tabelas.

    Sem ``tables``, compara as que existem nos dois schemas e aponta as que só
    existem em um. Colunas e tipos precisam coincidir.
    """
    in_a, in_b = _tables(conn, schema_a), _tables(conn, schema_b)
    names = sorted(tables) if tables else sorted(in_a | in_b)
    report = CompareReport()
    for name in names:
        diff = TableDiff(name)
        report.diffs.append(diff)
        if name not in in_a or name not in in_b:
            diff.note = f"só existe em {schema_b if name in in_b else schema_a}"
            continue
        cols_a, cols_b = _columns(conn, schema_a, name), _columns(conn, schema_b, name)
        if cols_a != cols_b:
            diff.note = f"colunas diferentes: {cols_a} vs {cols_b}"
            continue
        diff.rows_a = _count(conn, schema_a, name)
        diff.rows_b = _count(conn, schema_b, name)
        try:
            with conn.transaction():
                diff.only_a = _except_count(conn, schema_a, schema_b, name, cols_a)
                diff.only_b = _except_count(conn, schema_b, schema_a, name, cols_a)
        except psycopg.Error as exc:
            diff.note = f"não comparável: {str(exc).splitlines()[0]}"
    return report
