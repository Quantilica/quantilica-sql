"""Gerenciamento de configuração do quantilica-sql."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import platformdirs


@dataclass
class Settings:
    """Configurações centrais do motor quantilica-sql."""

    database_url: str = field(
        default_factory=lambda: os.environ.get(
            "DATABASE_URL", "postgresql://postgres@localhost:5432/dados"
        )
    )
    storage_root: Path = field(
        default_factory=lambda: Path(
            os.environ.get("QUANTILICA_STORAGE_ROOT", "./data")
        )
    )
    lake_dir: Path = field(
        default_factory=lambda: Path(
            os.environ.get("QUANTILICA_LAKE_DIR")
            or Path(os.environ.get("QUANTILICA_STORAGE_ROOT", "./data")).parent / "lake"
        )
    )
    staging_dir: Path = field(
        default_factory=lambda: Path(
            os.environ.get("QUANTILICA_STAGING_DIR", tempfile.gettempdir())
        )
    )
    analytics_schema: str = field(
        default_factory=lambda: os.environ.get(
            "QUANTILICA_ANALYTICS_SCHEMA", "analytics"
        )
    )
    tablespace_dados: str = field(
        default_factory=lambda: os.environ.get(
            "POSTGRES_TABLESPACE_DADOS", "pg_default"
        )
    )
    tablespace_rfb: str = field(
        default_factory=lambda: os.environ.get("POSTGRES_TABLESPACE_RFB", "pg_default")
    )
    plugins_dir: Path = field(
        default_factory=lambda: (
            platformdirs.user_data_path("quantilica-sql") / "plugins"
        )
    )

    def normalize_db_url(self) -> str:
        """Garante formato psycopg compatível (postgresql://)."""
        url = self.database_url
        if url.startswith("postgresql+psycopg://"):
            return url.replace("postgresql+psycopg://", "postgresql://")
        return url


_settings_instance: Settings | None = None


def get_settings() -> Settings:
    """Retorna instância singleton de configurações."""
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance
