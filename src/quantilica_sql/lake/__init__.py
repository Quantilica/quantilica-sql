"""DuckDB sobre o lake: views por dataset e carga do resultado no Postgres.

Requer o extra ``lake`` (``duckdb``).
"""

from quantilica_sql.lake.duck import (
    LakeError,
    connect_lake,
    lake_root,
    load_query_into_table,
    normalize_name,
    pg_type,
)

__all__ = [
    "LakeError",
    "connect_lake",
    "lake_root",
    "load_query_into_table",
    "normalize_name",
    "pg_type",
]
