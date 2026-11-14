"""Papel sem CREATE no banco não pode esbarrar em CREATE SCHEMA de schema existente."""

from __future__ import annotations

from quantilica_sql.database import create_schema_if_missing


class _Cursor:
    def __init__(self, existe: bool) -> None:
        self.existe = existe
        self.comandos: list[str] = []

    def execute(self, query, params=None) -> None:
        self.comandos.append(query if isinstance(query, str) else repr(query))

    def fetchone(self):
        return (1,) if self.existe else None


def test_schema_existente_nao_executa_create():
    cur = _Cursor(existe=True)
    create_schema_if_missing(cur, "tesouro_direto")
    assert len(cur.comandos) == 1
    assert "pg_namespace" in cur.comandos[0]


def test_schema_ausente_e_criado():
    cur = _Cursor(existe=False)
    create_schema_if_missing(cur, "novo")
    assert len(cur.comandos) == 2
    assert "CREATE SCHEMA" in cur.comandos[1]
