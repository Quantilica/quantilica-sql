"""Gerenciamento de repositórios de pipelines (plugins declarativos)."""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

from quantilica_sql.config import get_settings

logger = logging.getLogger("quantilica_sql.plugin_manager")


class PluginManager:
    """Gerencia clonagem, atualização e listagem de catálogos de pipelines."""

    def __init__(self, plugins_dir: Path | None = None) -> None:
        self.plugins_dir = plugins_dir or get_settings().plugins_dir
        self.plugins_dir.mkdir(parents=True, exist_ok=True)

    def list_plugins(self) -> list[dict[str, str]]:
        """Lista os catálogos de pipelines instalados."""
        plugins = []
        for p in self.plugins_dir.iterdir():
            if p.is_dir() and (p / "manifest.toml").exists():
                plugins.append({"alias": p.name, "path": str(p)})
        return plugins

    def install(self, repo_url: str, alias: str | None = None) -> Path:
        """Clona um repositório git de pipelines sob o alias indicado."""
        if not alias:
            alias = repo_url.rstrip("/").split("/")[-1].replace(".git", "")
        target_dir = self.plugins_dir / alias
        if target_dir.exists():
            logger.info("Atualizando plugin existente em %s...", target_dir)
            subprocess.run(
                ["git", "pull", "--ff-only"],
                cwd=target_dir,
                check=True,
                capture_output=True,
            )
        else:
            logger.info("Clonando %s para %s...", repo_url, target_dir)
            subprocess.run(
                ["git", "clone", repo_url, str(target_dir)],
                check=True,
                capture_output=True,
            )
        return target_dir

    def remove(self, alias: str) -> bool:
        """Remove um catálogo de pipelines instalado."""
        target_dir = self.plugins_dir / alias
        if target_dir.exists():
            shutil.rmtree(target_dir)
            return True
        return False
