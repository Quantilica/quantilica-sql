"""Validação estática de manifestos, fetch.toml e transform.toml de pipelines."""

from __future__ import annotations

import logging
import tomllib
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

logger = logging.getLogger("quantilica_sql.validator")

VALID_STRATEGIES = ("replace", "view")


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True)
class Issue:
    severity: Severity
    message: str
    file: Path | None = None

    def __str__(self) -> str:
        loc = f"{self.file}: " if self.file else ""
        return f"[{self.severity.value}] {loc}{self.message}"


@dataclass
class ValidationReport:
    issues: list[Issue] = field(default_factory=list)

    def error(self, message: str, file: Path | None = None) -> None:
        self.issues.append(Issue(Severity.ERROR, message, file))

    def warn(self, message: str, file: Path | None = None) -> None:
        self.issues.append(Issue(Severity.WARNING, message, file))

    def extend(self, other: ValidationReport) -> None:
        self.issues.extend(other.issues)

    @property
    def errors(self) -> list[Issue]:
        return [i for i in self.issues if i.severity is Severity.ERROR]

    @property
    def warnings(self) -> list[Issue]:
        return [i for i in self.issues if i.severity is Severity.WARNING]

    @property
    def ok(self) -> bool:
        return not self.errors


def _load_toml(path: Path, report: ValidationReport) -> dict[str, Any] | None:
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        report.error("arquivo não encontrado", path)
    except tomllib.TOMLDecodeError as exc:
        report.error(f"TOML inválido: {exc}", path)
    return None


def _is_str_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(v, str) for v in value)


class PipelineValidator:
    """Valida manifest.toml, fetch.toml e transform.toml."""

    # -- manifest.toml ---------------------------------------------------

    def validate_manifest(self, manifest_path: Path) -> ValidationReport:
        """Valida manifest nos formatos ``[catalog]+[pipelines.<id>]`` e
        ``[[pipeline]]``. Paths são relativos ao diretório do manifesto."""
        report = ValidationReport()
        data = _load_toml(manifest_path, report)
        if data is None:
            return report
        root = manifest_path.parent

        entries: list[tuple[str, Any]] = []
        if "pipelines" in data:
            pipelines = data["pipelines"]
            catalog = data.get("catalog")
            if not isinstance(catalog, dict):
                report.warn("seção [catalog] ausente", manifest_path)
            elif not catalog.get("name"):
                report.warn("[catalog] sem 'name'", manifest_path)
            if not isinstance(pipelines, dict) or not pipelines:
                report.error("[pipelines.<id>] ausente ou vazio", manifest_path)
            else:
                entries = list(pipelines.items())
        elif "pipeline" in data:
            arr = data["pipeline"]
            if not isinstance(arr, list) or not arr:
                report.error("[[pipeline]] deve ser um array não vazio", manifest_path)
            else:
                for key in ("name", "description", "version"):
                    if not data.get(key):
                        report.warn(f"chave de topo '{key}' ausente", manifest_path)
                seen: set[str] = set()
                for i, item in enumerate(arr):
                    pid = item.get("id") if isinstance(item, dict) else None
                    if not pid:
                        report.error(
                            f"[[pipeline]] #{i + 1}: 'id' ausente", manifest_path
                        )
                        continue
                    if pid in seen:
                        report.error(f"id duplicado: {pid!r}", manifest_path)
                    seen.add(pid)
                    entries.append((pid, item))
        else:
            report.error(
                "manifesto sem [pipelines.<id>] nem [[pipeline]]", manifest_path
            )
            return report

        for pid, item in entries:
            if not isinstance(item, dict):
                report.error(
                    f"pipeline {pid!r}: entrada deve ser uma tabela", manifest_path
                )
                continue
            rel = item.get("path")
            if not isinstance(rel, str) or not rel:
                report.error(f"pipeline {pid!r}: 'path' ausente", manifest_path)
                continue
            pdir = root / rel
            if not pdir.is_dir():
                report.error(
                    f"pipeline {pid!r}: path {rel!r} não existe", manifest_path
                )
            elif not any(
                p.exists()
                for name in ("fetch.toml", "transform.toml")
                for p in [pdir / name, *pdir.rglob(name)]
            ):
                report.error(
                    f"pipeline {pid!r}: {rel!r} sem fetch.toml/transform.toml",
                    manifest_path,
                )
        return report

    # -- fetch.toml --------------------------------------------------------

    def validate_fetch(self, fetch_path: Path) -> ValidationReport:
        """Aceita ``[[tabelas]]`` (SIDRA), ``[[series]]`` (BCB) e ``[pipeline]``
        (cross-domain declarativo)."""
        report = ValidationReport()
        data = _load_toml(fetch_path, report)
        if data is None:
            return report

        tabelas, series, pipeline = (
            data.get("tabelas"),
            data.get("series"),
            data.get("pipeline"),
        )
        if tabelas is None and series is None and pipeline is None:
            report.error("sem [[tabelas]], [[series]] ou [pipeline]", fetch_path)
            return report

        if tabelas is not None:
            if not isinstance(tabelas, list) or not tabelas:
                report.error("[[tabelas]] deve ser array não vazio", fetch_path)
            else:
                for i, t in enumerate(tabelas, 1):
                    if not isinstance(t, dict) or not t.get("tabela_sidra"):
                        report.error(
                            f"[[tabelas]] #{i}: 'tabela_sidra' ausente", fetch_path
                        )

        if series is not None:
            if not isinstance(series, list) or not series:
                report.error("[[series]] deve ser array não vazio", fetch_path)
            else:
                for i, s in enumerate(series, 1):
                    if not isinstance(s, dict):
                        report.error(f"[[series]] #{i}: deve ser tabela", fetch_path)
                        continue
                    ids, themes = s.get("ids"), s.get("themes")
                    if ids is None and themes is None:
                        report.error(
                            f"[[series]] #{i}: requer 'ids' ou 'themes'", fetch_path
                        )
                    if ids is not None and not (
                        isinstance(ids, list)
                        and ids
                        and all(
                            isinstance(v, int) and not isinstance(v, bool) for v in ids
                        )
                    ):
                        report.error(
                            f"[[series]] #{i}: 'ids' deve ser lista de int", fetch_path
                        )
                    if themes is not None and not (_is_str_list(themes) and themes):
                        report.error(
                            f"[[series]] #{i}: 'themes' deve ser lista de str",
                            fetch_path,
                        )

        if pipeline is not None:
            if not isinstance(pipeline, dict):
                report.error("[pipeline] deve ser uma tabela", fetch_path)
            else:
                if not pipeline.get("name"):
                    report.error("[pipeline] sem 'name'", fetch_path)
                if "domains" in pipeline and not _is_str_list(pipeline["domains"]):
                    report.error(
                        "[pipeline] 'domains' deve ser lista de str", fetch_path
                    )
        return report

    # -- transform.toml ----------------------------------------------------

    def validate_transform(self, transform_path: Path) -> ValidationReport:
        report = ValidationReport()
        data = _load_toml(transform_path, report)
        if data is None:
            return report

        tables = data.get("table")
        if isinstance(tables, dict):
            report.error(
                "[table] singular não suportado; use [[table]]", transform_path
            )
            return report
        if not isinstance(tables, list) or not tables:
            report.error("[[table]] ausente ou vazio", transform_path)
            return report

        seen: set[tuple[str, str]] = set()
        for i, t in enumerate(tables, 1):
            tag = f"[[table]] #{i}"
            if not isinstance(t, dict):
                report.error(f"{tag}: deve ser tabela", transform_path)
                continue
            for key in ("name", "schema", "strategy", "sql"):
                if not isinstance(t.get(key), str) or not t[key]:
                    report.error(
                        f"{tag}: '{key}' ausente ou não-string", transform_path
                    )
            name, schema = t.get("name"), t.get("schema")
            if isinstance(name, str) and isinstance(schema, str):
                if (schema, name) in seen:
                    report.error(f"{tag}: {schema}.{name} duplicado", transform_path)
                seen.add((schema, name))
            strategy = t.get("strategy")
            if isinstance(strategy, str) and strategy not in VALID_STRATEGIES:
                report.error(
                    f"{tag}: strategy {strategy!r} inválida (use {VALID_STRATEGIES})",
                    transform_path,
                )
            sql_rel = t.get("sql")
            if isinstance(sql_rel, str) and sql_rel:
                if not (transform_path.parent / sql_rel).is_file():
                    report.error(
                        f"{tag}: arquivo sql {sql_rel!r} não existe", transform_path
                    )

            pk = t.get("primary_key")
            if pk is not None:
                if not (_is_str_list(pk) and pk):
                    report.error(
                        f"{tag}: 'primary_key' deve ser lista de str", transform_path
                    )
                if strategy == "view":
                    report.warn(
                        f"{tag}: 'primary_key' ignorada em view", transform_path
                    )
            idx = t.get("indexes")
            if idx is not None:
                if not isinstance(idx, list):
                    report.error(
                        f"{tag}: 'indexes' deve ser lista de tabelas", transform_path
                    )
                else:
                    if strategy == "view" and idx:
                        report.warn(
                            f"{tag}: 'indexes' ignorado em view", transform_path
                        )
                    idx_names: set[str] = set()
                    for j, ix in enumerate(idx, 1):
                        itag = f"{tag} indexes #{j}"
                        if not isinstance(ix, dict):
                            report.error(f"{itag}: deve ser tabela", transform_path)
                            continue
                        iname = ix.get("name")
                        if not isinstance(iname, str) or not iname:
                            report.error(f"{itag}: 'name' ausente", transform_path)
                        elif iname in idx_names:
                            report.error(
                                f"{itag}: nome {iname!r} duplicado", transform_path
                            )
                        else:
                            idx_names.add(iname)
                        cols = ix.get("columns")
                        if not (_is_str_list(cols) and cols):
                            report.error(
                                f"{itag}: 'columns' deve ser lista de str",
                                transform_path,
                            )
                        if "unique" in ix and not isinstance(ix["unique"], bool):
                            report.error(
                                f"{itag}: 'unique' deve ser bool", transform_path
                            )
        return report

    # -- diretório de pipeline ----------------------------------------------

    def validate_pipeline_dir(self, pipeline_dir: Path) -> ValidationReport:
        report = ValidationReport()
        if not pipeline_dir.is_dir():
            report.error("não é um diretório", pipeline_dir)
            return report
        fetch, transform = pipeline_dir / "fetch.toml", pipeline_dir / "transform.toml"
        if not fetch.exists() and not transform.exists():
            report.error("sem fetch.toml nem transform.toml", pipeline_dir)
        if fetch.exists():
            report.extend(self.validate_fetch(fetch))
        if transform.exists():
            report.extend(self.validate_transform(transform))
        return report

    def validate_catalog(self, root: Path) -> ValidationReport:
        """Valida manifest.toml e todos os pipelines (fetch/transform) sob ``root``."""
        report = ValidationReport()
        manifest = root / "manifest.toml"
        if manifest.exists():
            report.extend(self.validate_manifest(manifest))
        else:
            report.warn("manifest.toml ausente", root)
        for d in sorted(
            {
                p.parent
                for name in ("fetch.toml", "transform.toml")
                for p in root.rglob(name)
            }
        ):
            report.extend(self.validate_pipeline_dir(d))
        return report


def find_pipeline_dirs(root: Path) -> list[Path]:
    """Diretórios sob ``root`` com fetch.toml ou transform.toml."""
    return sorted(
        {
            p.parent
            for name in ("fetch.toml", "transform.toml")
            for p in root.rglob(name)
        }
    )
