"""DuckDB sobre o lake: views por dataset e carga do resultado no Postgres."""

from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.config import get_settings
from quantilica_sql.lake.convert import _lit, lake_root
from quantilica_sql.lake.specs import get_spec, normalize_name

_NULL = "\\N"

_SIMPLE_TYPES = {
    "TINYINT": "smallint",
    "SMALLINT": "smallint",
    "INTEGER": "integer",
    "BIGINT": "bigint",
    "HUGEINT": "numeric",
    "UTINYINT": "smallint",
    "USMALLINT": "integer",
    "UINTEGER": "bigint",
    "UBIGINT": "numeric(20,0)",
    "UHUGEINT": "numeric",
    "FLOAT": "real",
    "DOUBLE": "double precision",
    "VARCHAR": "text",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIME": "time",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamptz",
    "BLOB": "bytea",
    "UUID": "uuid",
    "INTERVAL": "interval",
}
_DECIMAL = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$")


def pg_type(duck_type: str) -> str:
    """Tipo do DuckDB → tipo do PostgreSQL (``text`` para o que não tem par direto)."""
    t = duck_type.upper()
    if t in _SIMPLE_TYPES:
        return _SIMPLE_TYPES[t]
    m = _DECIMAL.match(t)
    if m:
        return f"numeric({m.group(1)},{m.group(2)})"
    return "text"


def connect_lake(lake: Path | None = None):
    """Conexão DuckDB em memória com uma view por dataset: ``<fonte>.<dataset>``.

    A fonte e o dataset são normalizados (``rfb-cnpj`` → ``rfb_cnpj``); datasets com
    apelido na especificação usam o apelido (``tesouro_direto.investidores``). Cada view
    expõe ``_arquivo`` (nome do Parquet de origem) além das colunas do dataset.
    """
    import duckdb

    root = lake_root(lake)
    conn = duckdb.connect()
    conn.execute("SET TimeZone = 'UTC'")
    tmp = root / ".tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    conn.execute(f"SET temp_directory = {_lit(str(tmp))}")
    memory = os.environ.get("QUANTILICA_DUCKDB_MEMORY")
    if memory:
        conn.execute(f"SET memory_limit = {_lit(memory)}")
    threads = os.environ.get("QUANTILICA_DUCKDB_THREADS")
    if threads:
        conn.execute(f"SET threads = {int(threads)}")

    if not root.exists():
        return conn
    for source_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        if source_dir.name.startswith("."):
            continue
        schema = normalize_name(source_dir.name)
        for dataset_dir in sorted(p for p in source_dir.iterdir() if p.is_dir()):
            if not any(dataset_dir.rglob("*.parquet")):
                continue
            spec = get_spec(source_dir.name, dataset_dir.name)
            view = spec.view_name if spec else normalize_name(dataset_dir.name)
            glob = _lit(f"{dataset_dir}/**/*.parquet")
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            conn.execute(
                f'CREATE OR REPLACE VIEW "{schema}"."{view}" AS '
                f"SELECT * EXCLUDE (filename), regexp_extract(filename, '[^/]+$') "
                f"AS _arquivo FROM read_parquet({glob}, union_by_name=true, "
                "filename=true)"
            )
    return conn


def load_query_into_table(
    duck: Any,
    query: str,
    cur: psycopg.Cursor,
    schema: str,
    table: str,
    tmp_dir: Path | None = None,
) -> int:
    """Executa ``query`` no DuckDB e cria ``schema.table`` no Postgres com o resultado.

    O resultado vai por um CSV temporário (NULL como ``\\N``, que distingue NULL de
    texto vazio) e é carregado por COPY. Não commita: roda na transação de quem chama.
    """
    query = query.strip().rstrip(";").rstrip()
    described = duck.execute(f"DESCRIBE {query}").fetchall()
    columns = [(row[0], pg_type(row[1])) for row in described]
    ddl = sql.SQL("CREATE TABLE {} ({})").format(
        sql.Identifier(schema, table),
        sql.SQL(", ").join(
            sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(typ))
            for name, typ in columns
        ),
    )
    cur.execute(ddl)

    base = tmp_dir or get_settings().lake_dir / ".tmp"
    base.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=base) as work:
        csv_path = Path(work) / "result.csv"
        duck.execute(
            f"COPY ({query}) TO {_lit(str(csv_path))} "
            f"(FORMAT csv, HEADER false, DELIMITER ',', NULLSTR {_lit(_NULL)})"
        )
        copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT csv, NULL {})").format(
            sql.Identifier(schema, table), sql.Literal(_NULL)
        )
        with cur.copy(copy_sql) as copy:
            with open(csv_path, "rb") as f:
                while chunk := f.read(1 << 20):
                    copy.write(chunk)
        rows = cur.rowcount
    return rows if rows is not None and rows >= 0 else 0
