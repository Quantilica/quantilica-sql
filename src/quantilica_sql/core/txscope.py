"""Escopo transacional compartilhado por ``atomic`` e pelos utilitários de carga."""

from __future__ import annotations

import contextvars
from dataclasses import dataclass, field

import psycopg


@dataclass
class Scope:
    """Carga atômica em curso: o schema, o arquivo e as tabelas que ela tocou."""

    schema: str
    filename: str
    sha256: str | None = None
    touched: set[str] = field(default_factory=set)


ACTIVE: contextvars.ContextVar[Scope | None] = contextvars.ContextVar(
    "quantilica_sql_atomic_scope", default=None
)


def in_atomic_scope() -> bool:
    """Indica se há uma carga atômica em curso (quem chama não deve commitar)."""
    return ACTIVE.get() is not None


def commit_if_standalone(conn: psycopg.Connection) -> None:
    """Commita como antes, exceto dentro de ``atomic_file_load``."""
    if not conn.autocommit and not in_atomic_scope():
        conn.commit()
