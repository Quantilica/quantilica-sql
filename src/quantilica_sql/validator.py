"""Validação estática de manifestos e configurações de pipelines."""

from __future__ import annotations

import logging
import tomllib
from pathlib import Path
from typing import Any

logger = logging.getLogger("quantilica_sql.validator")


class ManifestValidator:
    """Valida a conformidade de catálogos declarativos e arquivos TOML."""

    @staticmethod
    def validate_manifest(manifest_path: Path) -> dict[str, Any]:
        """Lê e valida um manifest.toml."""
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifesto não encontrado: {manifest_path}")

        with open(manifest_path, "rb") as f:
            data = tomllib.load(f)

        if "pipelines" not in data and "pipeline" not in data:
            raise ValueError(
                f"Manifesto inválido em {manifest_path}: chave 'pipelines' ausente."
            )
        return data

    @staticmethod
    def validate_pipeline_dir(pipeline_dir: Path) -> dict[str, Any]:
        """Valida que o diretório de um pipeline contém os arquivos necessários."""
        if not pipeline_dir.is_dir():
            raise NotADirectoryError(f"Caminho não é diretório: {pipeline_dir}")

        fetch_toml = pipeline_dir / "fetch.toml"
        has_fetch = fetch_toml.exists()

        sql_files = list(pipeline_dir.glob("*.sql"))
        has_sql = len(sql_files) > 0

        return {
            "name": pipeline_dir.name,
            "has_fetch": has_fetch,
            "sql_files": [f.name for f in sql_files],
            "is_valid": has_sql,
        }
