"""Lake de Parquet para microdados que não vivem no PostgreSQL.

Requer o extra ``lake`` (``duckdb``).
"""

from quantilica_sql.lake.convert import (
    ConvertResult,
    ConvertSummary,
    LakeError,
    convert_file,
    convert_tree,
    file_key,
    lake_root,
    origin_matches,
    output_path,
    partition_of,
)
from quantilica_sql.lake.specs import DatasetSpec, get_spec, normalize_name, sources

__all__ = [
    "ConvertResult",
    "ConvertSummary",
    "DatasetSpec",
    "LakeError",
    "convert_file",
    "convert_tree",
    "file_key",
    "get_spec",
    "lake_root",
    "normalize_name",
    "origin_matches",
    "output_path",
    "partition_of",
    "sources",
]
