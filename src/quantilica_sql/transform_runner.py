"""Execução de transform.toml: materialização de tabelas/views na camada analytics."""

from __future__ import annotations

import logging
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

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

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}"


def load_table_specs(
    transform_toml: Path, target_schema: str | None = None
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
    ) -> None:
        # Schema de origem (raw) colocado no search_path; o destino é sempre
        # qualificado via ``schema`` de cada [[table]] (ou por target_schema).
        self.raw_schema = raw_schema
        self.target_schema = target_schema

    def run_transform_toml(
        self, conn: psycopg.Connection, transform_toml: Path
    ) -> list[str]:
        """Executa todas as entradas; retorna ``schema.name`` das materializadas."""
        done = []
        for spec in load_table_specs(transform_toml, self.target_schema):
            self.run_table(conn, spec)
            done.append(spec.qualified)
        return done

    def run_table(self, conn: psycopg.Connection, spec: TableSpec) -> None:
        query = _read_query(spec.sql_path)
        logger.info("Materializando %s (%s)", spec.qualified, spec.strategy)
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
            if spec.strategy == "replace":
                self._replace(cur, spec, target, query)
            elif spec.strategy == "view":
                self._view(cur, target, query)
            else:
                raise ValueError(f"Estratégia desconhecida: {spec.strategy!r}")

    @staticmethod
    def _replace(
        cur: psycopg.Cursor, spec: TableSpec, target: sql.Identifier, query: sql.SQL
    ) -> None:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target))
        cur.execute(sql.SQL("CREATE TABLE {} AS ").format(target) + query)
        if spec.primary_key:
            cur.execute(
                sql.SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(
                    target, sql.SQL(", ").join(map(sql.Identifier, spec.primary_key))
                )
            )
        for ix in spec.indexes:
            cur.execute(
                sql.SQL("CREATE {}INDEX {} ON {} ({})").format(
                    sql.SQL("UNIQUE " if ix.unique else ""),
                    sql.Identifier(ix.name),
                    target,
                    sql.SQL(", ").join(map(sql.Identifier, ix.columns)),
                )
            )

    @staticmethod
    def _view(cur: psycopg.Cursor, target: sql.Identifier, query: sql.SQL) -> None:
        cur.execute(sql.SQL("DROP VIEW IF EXISTS {} CASCADE").format(target))
        cur.execute(sql.SQL("CREATE VIEW {} AS ").format(target) + query)
