"""Orquestrador de execução de pipelines declarativos (transform.toml)."""

from __future__ import annotations

import logging
import tomllib
from pathlib import Path
from typing import Any

from quantilica_sql.database import get_connection
from quantilica_sql.plugin_manager import PluginManager
from quantilica_sql.transform_runner import TransformRunner

logger = logging.getLogger("quantilica_sql.runner")

WORKSPACE_CATALOG = "quantilica-pipelines"


def _has_transform(path: Path) -> bool:
    return path.is_dir() and any(path.rglob("transform.toml"))


def _manifest_lookup(root: Path, name: str) -> Path | None:
    """Resolve ``name`` como id no manifest.toml (``[pipelines.<id>]`` ou
    ``[[pipeline]]``)."""
    manifest = root / "manifest.toml"
    if not manifest.is_file():
        return None
    with open(manifest, "rb") as f:
        data = tomllib.load(f)
    entry = data.get("pipelines", {}).get(name)
    if entry is None:
        entry = next((p for p in data.get("pipeline", []) if p.get("id") == name), None)
    if isinstance(entry, dict) and isinstance(entry.get("path"), str):
        return root / entry["path"]
    return None


def find_pipeline(pipeline_name: str) -> Path | None:
    """Busca o pipeline como caminho, id/caminho no catálogo do workspace ou
    nos plugins instalados."""
    p = Path(pipeline_name)
    if _has_transform(p):
        return p

    roots = [Path(WORKSPACE_CATALOG)]
    roots += [Path(plug["path"]) for plug in PluginManager().list_plugins()]
    for root in roots:
        for candidate in (_manifest_lookup(root, pipeline_name), root / pipeline_name):
            if candidate is not None and _has_transform(candidate):
                return candidate
    return None


def infer_raw_schema(pipeline_dir: Path) -> str | None:
    """Schema raw a partir do fetch.toml: ``[[tabelas]]`` → ibge_sidra,
    ``[[series]]`` → bcb_sgs; cross-domain (``[pipeline]``) → None."""
    fetch = pipeline_dir / "fetch.toml"
    if not fetch.is_file():
        return None
    with open(fetch, "rb") as f:
        data = tomllib.load(f)
    if "tabelas" in data:
        return "ibge_sidra"
    if "series" in data:
        return "bcb_sgs"
    return None


def iter_transform_dirs(root: Path) -> list[Path]:
    """Ordem de execução do run_subtree: filhos primeiro, depois o próprio dir."""
    ordered: list[Path] = []
    for child in sorted(p for p in root.iterdir() if p.is_dir()):
        if _has_transform(child):
            ordered.extend(iter_transform_dirs(child))
    if (root / "transform.toml").is_file():
        ordered.append(root)
    return ordered


def engine_for(pipeline_dir: Path) -> str:
    """Engine padrão do pipeline: o da entrada do manifest que o cobre (ou postgres)."""
    target = pipeline_dir.resolve()
    roots = [Path(WORKSPACE_CATALOG)]
    roots += [Path(plug["path"]) for plug in PluginManager().list_plugins()]
    for root in roots:
        manifest = root / "manifest.toml"
        if not manifest.is_file():
            continue
        with open(manifest, "rb") as f:
            data = tomllib.load(f)
        entries = list(data.get("pipelines", {}).values()) + list(
            data.get("pipeline", [])
        )
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
                continue
            base = (root / entry["path"]).resolve()
            if target == base or base in target.parents:
                return entry.get("engine", "postgres")
    return "postgres"


def run_pipeline(
    pipeline_name: str,
    raw_schema: str | None = None,
    target_schema: str | None = None,
) -> dict[str, Any]:
    """Executa os transform.toml de um pipeline (e subpipelines)."""
    pipeline_dir = find_pipeline(pipeline_name)
    if not pipeline_dir:
        raise FileNotFoundError(
            f"Pipeline '{pipeline_name}' não encontrado. "
            "Verifique o nome ou instale o plugin correspondente."
        )

    logger.info("Executando pipeline em %s...", pipeline_dir)
    materialized: list[str] = []
    with get_connection() as conn:
        for d in iter_transform_dirs(pipeline_dir):
            runner = TransformRunner(
                raw_schema or infer_raw_schema(d),
                target_schema=target_schema,
                default_engine=engine_for(d),
            )
            materialized.extend(runner.run_transform_toml(conn, d / "transform.toml"))

    return {
        "status": "success",
        "pipeline": pipeline_name,
        "materialized": materialized,
    }
