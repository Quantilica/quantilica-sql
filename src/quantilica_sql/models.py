"""Definições de modelos e estruturas de dados do quantilica-sql."""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LoadedFileRecord:
    """Registro de idempotência para arquivo já carregado."""

    filename: str
    dataset: str
    rows_loaded: int
    loaded_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TableInfo:
    """Informações estruturais de uma tabela."""

    schema: str
    name: str
    columns: list[str]
    row_count: int = 0
    total_size: str = "0 bytes"
    is_partitioned: bool = False
    tablespace: str = "pg_default"


@dataclass
class PipelineConfig:
    """Configuração de um pipeline declarativo."""

    name: str
    domain: str
    description: str = ""
    fetch_config: dict[str, Any] = field(default_factory=dict)
    transform_config: dict[str, Any] = field(default_factory=dict)
    sql_path: str = ""
