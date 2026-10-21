"""Orquestrador de execução de pipelines declarativos."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from quantilica_sql.config import get_settings
from quantilica_sql.database import get_connection
from quantilica_sql.plugin_manager import PluginManager
from quantilica_sql.transform_runner import TransformRunner

logger = logging.getLogger("quantilica_sql.runner")


def find_pipeline(pipeline_name: str) -> Path | None:
    """Busca o pipeline em diretórios locais ou nos plugins instalados."""
    # 1. Verifica se é caminho direto
    p = Path(pipeline_name)
    if p.is_dir() and any(p.glob("*.sql")):
        return p

    # 2. Busca no repositório de pipelines do workspace se existir
    workspace_pipeline = Path(f"quantilica-pipelines/{pipeline_name}")
    if workspace_pipeline.is_dir() and any(workspace_pipeline.glob("*.sql")):
        return workspace_pipeline

    # 3. Busca nos plugins instalados
    mgr = PluginManager()
    for plug in mgr.list_plugins():
        target = Path(plug["path"]) / pipeline_name
        if target.is_dir() and any(target.glob("*.sql")):
            return target
    return None


def run_pipeline(pipeline_name: str) -> dict[str, Any]:
    """Executa um pipeline por nome."""
    pipeline_dir = find_pipeline(pipeline_name)
    if not pipeline_dir:
        raise FileNotFoundError(
            f"Pipeline '{pipeline_name}' não encontrado. "
            "Verifique o nome ou instale o plugin correspondente."
        )

    logger.info("Executando pipeline em %s...", pipeline_dir)
    sql_files = list(pipeline_dir.glob("*.sql"))
    if not sql_files:
        raise FileNotFoundError(f"Nenhum arquivo SQL encontrado em {pipeline_dir}")

    runner = TransformRunner(get_settings().analytics_schema)
    with get_connection() as conn:
        for sql_f in sql_files:
            runner.execute_sql_file(conn, sql_f)

    return {
        "status": "success",
        "pipeline": pipeline_name,
        "executed_sql": [f.name for f in sql_files],
    }
