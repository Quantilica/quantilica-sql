"""Execução de transform.toml: materialização de tabelas/views na camada analytics."""

from __future__ import annotations

import logging
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.validator import PipelineValidator

logger = logging.getLogger("quantilica_sql.transform_runner")


@dataclass(frozen=True)
class IndexSpec:
    name: str
    columns: tuple[str, ...]
    unique: bool = False


@dataclass(frozen=True)
class TableSpec:
    name: str
    schema: str
    strategy: str
    sql_path: Path
    primary_key: tuple[str, ...] = ()
    indexes: tuple[IndexSpec, ...] = field(default_factory=tuple)
    engine: str = "postgres"

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}"


_NEW = "__new"
_OLD = "__old"
_DROP_KIND = {
    "r": "TABLE",
    "p": "TABLE",
    "m": "MATERIALIZED VIEW",
    "v": "VIEW",
}


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


@dataclass(frozen=True)
class _DependentView:
    oid: int
    schema: str
    name: str
    definition: str


_DEPENDENTS_SQL = """
SELECT DISTINCT c.oid, n.nspname, c.relname, c.relkind
FROM pg_depend d
JOIN pg_rewrite r ON r.oid = d.objid
JOIN pg_class c ON c.oid = r.ev_class
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE d.classid = 'pg_rewrite'::regclass
  AND d.refobjid = ANY(%s)
  AND c.oid <> d.refobjid
  AND c.relkind IN ('v', 'm')
"""

_EDGES_SQL = """
SELECT DISTINCT c.oid, d.refobjid
FROM pg_depend d
JOIN pg_rewrite r ON r.oid = d.objid
JOIN pg_class c ON c.oid = r.ev_class
WHERE d.classid = 'pg_rewrite'::regclass
  AND c.oid = ANY(%s)
  AND d.refobjid = ANY(%s)
  AND c.oid <> d.refobjid
"""


def _dependent_views(
    cur: psycopg.Cursor, schema: str, name: str
) -> list[_DependentView]:
    """Views que dependem (direta ou indiretamente) da tabela, em ordem de criação.

    A definição é lida com ``pg_get_viewdef``; permissões (GRANT) das views não são
    preservadas. Materialized views dependentes não são recriadas: o erro é
    explícito para o dado não sumir em silêncio.
    """
    cur.execute("SELECT to_regclass(%s)::oid", (f"{_quote(schema)}.{_quote(name)}",))
    root = cur.fetchone()[0]
    found: dict[int, tuple[str, str]] = {}
    frontier = [root]
    while frontier:
        cur.execute(_DEPENDENTS_SQL, (frontier,))
        frontier = []
        for oid, nsp, rel, kind in cur.fetchall():
            if oid in found:
                continue
            if kind == "m":
                raise ValueError(
                    f"{schema}.{name} tem a materialized view {nsp}.{rel} como "
                    "dependente; remova-a antes de reconstruir a tabela."
                )
            found[oid] = (nsp, rel)
            frontier.append(oid)
    if not found:
        return []

    ids = list(found)
    cur.execute(_EDGES_SQL, (ids, ids))
    needs: dict[int, set[int]] = {oid: set() for oid in ids}
    for view_oid, ref in cur.fetchall():
        needs[view_oid].add(ref)
    ordered: list[int] = []
    pending = dict(needs)
    while pending:
        ready = sorted(oid for oid, deps in pending.items() if deps <= set(ordered))
        if not ready:  # ciclo é impossível em views; evita laço infinito
            raise ValueError(f"dependência circular entre views de {schema}.{name}")
        for oid in ready:
            ordered.append(oid)
            del pending[oid]

    views = []
    for oid in ordered:
        cur.execute("SELECT pg_get_viewdef(%s::oid, true)", (oid,))
        views.append(_DependentView(oid, *found[oid], cur.fetchone()[0]))
    return views


def load_table_specs(
    transform_toml: Path,
    target_schema: str | None = None,
    default_engine: str = "postgres",
) -> list[TableSpec]:
    """Valida e carrega as entradas ``[[table]]`` de um transform.toml."""
    report = PipelineValidator().validate_transform(transform_toml)
    if not report.ok:
        raise ValueError(
            f"transform.toml inválido ({transform_toml}):\n"
            + "\n".join(f"  - {i.message}" for i in report.errors)
        )
    with open(transform_toml, "rb") as f:
        data = tomllib.load(f)
    return [
        TableSpec(
            name=t["name"],
            schema=target_schema or t["schema"],
            strategy=t["strategy"],
            sql_path=transform_toml.parent / t["sql"],
            engine=t.get("engine", default_engine),
            primary_key=tuple(t.get("primary_key", ())),
            indexes=tuple(
                IndexSpec(ix["name"], tuple(ix["columns"]), ix.get("unique", False))
                for ix in t.get("indexes", ())
            ),
        )
        for t in data["table"]
    ]


def _read_query(path: Path) -> sql.SQL:
    """Lê o SELECT do arquivo. Sem parâmetros na execução, então ``%`` é literal."""
    text = path.read_text(encoding="utf-8").strip().rstrip(";").rstrip()
    if not text:
        raise ValueError(f"Arquivo SQL vazio: {path}")
    return sql.SQL(text)  # type: ignore[arg-type]


class TransformRunner:
    """Executa as entradas de um transform.toml, cada uma em transação própria."""

    def __init__(
        self,
        raw_schema: str | None = None,
        target_schema: str | None = None,
        lake: Path | None = None,
        default_engine: str = "postgres",
    ) -> None:
        # Schema de origem (raw) colocado no search_path; o destino é sempre
        # qualificado via ``schema`` de cada [[table]] (ou por target_schema).
        self.raw_schema = raw_schema
        self.target_schema = target_schema
        self.lake = lake
        self.default_engine = default_engine

    def run_transform_toml(
        self, conn: psycopg.Connection, transform_toml: Path
    ) -> list[str]:
        """Executa todas as entradas; retorna ``schema.name`` das materializadas.

        Entradas ``engine = "duckdb"`` rodam sobre o lake, numa conexão DuckDB
        compartilhada pelo arquivo (e preparada pelo ``prelude``, se houver).
        """
        specs = load_table_specs(
            transform_toml, self.target_schema, self.default_engine
        )
        duck = None
        done = []
        try:
            for spec in specs:
                if spec.engine == "duckdb" and duck is None:
                    duck = self._open_duck(transform_toml)
                self.run_table(conn, spec, duck=duck)
                done.append(spec.qualified)
        finally:
            if duck is not None:
                duck.close()
        return done

    def _open_duck(self, transform_toml: Path):
        from quantilica_sql.lake.duck import connect_lake

        duck = connect_lake(self.lake)
        with open(transform_toml, "rb") as f:
            prelude = tomllib.load(f).get("prelude")
        if prelude:
            text = (transform_toml.parent / prelude).read_text(encoding="utf-8")
            duck.execute(text)
        return duck

    def run_table(
        self, conn: psycopg.Connection, spec: TableSpec, duck: Any = None
    ) -> None:
        query = _read_query(spec.sql_path)
        logger.info(
            "Materializando %s (%s, %s)", spec.qualified, spec.strategy, spec.engine
        )
        # Bloco transacional por entrada: rollback isola falhas.
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    sql.Identifier(spec.schema)
                )
            )
            if self.raw_schema:
                cur.execute(
                    sql.SQL("SET LOCAL search_path TO {}, public").format(
                        sql.Identifier(self.raw_schema)
                    )
                )
            target = sql.Identifier(spec.schema, spec.name)
            if spec.engine == "duckdb":
                if spec.strategy != "replace":
                    raise ValueError(
                        f"{spec.qualified}: engine duckdb só aceita strategy replace"
                    )
                self._replace_duckdb(cur, spec, target, duck)
            elif spec.engine != "postgres":
                raise ValueError(f"Engine desconhecido: {spec.engine!r}")
            elif spec.strategy == "replace":
                self._replace(cur, spec, target, query)
            elif spec.strategy == "view":
                self._view(cur, target, query)
            else:
                raise ValueError(f"Estratégia desconhecida: {spec.strategy!r}")

    @staticmethod
    def _replace(
        cur: psycopg.Cursor, spec: TableSpec, target: sql.Identifier, query: sql.SQL
    ) -> None:
        """Reconstrói a tabela sem bloquear leituras durante a construção.

        A tabela nova (``<nome>__new``) é montada com dados, PK e índices antes de
        qualquer lock no destino; só então as views dependentes são removidas, os
        nomes trocados e as views recriadas. O ``ACCESS EXCLUSIVE`` dura só a troca.
        """
        new = sql.Identifier(spec.schema, f"{spec.name}{_NEW}")
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(new))
        cur.execute(sql.SQL("CREATE TABLE {} AS ").format(new) + query)
        TransformRunner._swap_in(cur, spec, target)

    @staticmethod
    def _replace_duckdb(
        cur: psycopg.Cursor, spec: TableSpec, target: sql.Identifier, duck: Any
    ) -> None:
        """Mesma troca atômica, com a tabela nova vinda de uma consulta DuckDB."""
        from quantilica_sql.lake.duck import load_query_into_table

        new = sql.Identifier(spec.schema, f"{spec.name}{_NEW}")
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(new))
        query = spec.sql_path.read_text(encoding="utf-8")
        load_query_into_table(duck, query, cur, spec.schema, f"{spec.name}{_NEW}")
        TransformRunner._swap_in(cur, spec, target)

    @staticmethod
    def _swap_in(cur: psycopg.Cursor, spec: TableSpec, target: sql.Identifier) -> None:
        """PK e índices na tabela nova e troca de nomes (``<nome>__new`` → ``<nome>``).

        O destino pode ser tabela, view ou materialized view; é removido na troca.
        """
        schema, name = spec.schema, spec.name
        new = sql.Identifier(schema, f"{name}{_NEW}")
        if spec.primary_key:
            cur.execute(
                sql.SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(
                    new, sql.SQL(", ").join(map(sql.Identifier, spec.primary_key))
                )
            )
        for ix in spec.indexes:
            cur.execute(
                sql.SQL("CREATE {}INDEX {} ON {} ({})").format(
                    sql.SQL("UNIQUE " if ix.unique else ""),
                    sql.Identifier(f"{ix.name}{_NEW}"),
                    new,
                    sql.SQL(", ").join(map(sql.Identifier, ix.columns)),
                )
            )

        cur.execute(
            "SELECT relkind FROM pg_class WHERE oid = to_regclass(%s)",
            (f"{_quote(schema)}.{_quote(name)}",),
        )
        row = cur.fetchone()
        kind = row[0] if row is not None else None
        exists = kind is not None
        dependents = _dependent_views(cur, schema, name) if exists else []
        for dep in reversed(dependents):
            cur.execute(
                sql.SQL("DROP VIEW {}").format(sql.Identifier(dep.schema, dep.name))
            )
        old = sql.Identifier(schema, f"{name}{_OLD}")
        if exists:
            cur.execute(
                sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                    target, sql.Identifier(f"{name}{_OLD}")
                )
            )
        cur.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(new, sql.Identifier(name))
        )
        if exists:
            drop = _DROP_KIND.get(kind, "TABLE")
            cur.execute(sql.SQL("DROP {} {}").format(sql.SQL(drop), old))
        if spec.primary_key:
            cur.execute(
                sql.SQL("ALTER INDEX {} RENAME TO {}").format(
                    sql.Identifier(schema, f"{name}{_NEW}_pkey"),
                    sql.Identifier(f"{name}_pkey"),
                )
            )
        for ix in spec.indexes:
            cur.execute(
                sql.SQL("ALTER INDEX {} RENAME TO {}").format(
                    sql.Identifier(schema, f"{ix.name}{_NEW}"),
                    sql.Identifier(ix.name),
                )
            )
        for dep in dependents:
            cur.execute(
                sql.SQL("CREATE VIEW {} AS ").format(
                    sql.Identifier(dep.schema, dep.name)
                )
                + sql.SQL(dep.definition.rstrip().rstrip(";"))  # type: ignore[arg-type]
            )

    @staticmethod
    def _view(cur: psycopg.Cursor, target: sql.Identifier, query: sql.SQL) -> None:
        # sem CASCADE: dependentes viram erro explícito, não somem em silêncio
        cur.execute(sql.SQL("DROP VIEW IF EXISTS {}").format(target))
        cur.execute(sql.SQL("CREATE VIEW {} AS ").format(target) + query)
