"""Conversão do arquivo bruto (CSV/TXT, ZIP, 7z) em Parquet, em streaming.

Um Parquet por arquivo de origem, em
``<lake>/<fonte>/<dataset>/<partição>/<chave>.parquet``.
A chave é o nome do arquivo sem a data de publicação (``@AAAAMMDD``), de modo que uma
republicação substitui o Parquet anterior em vez de acumular versões. A proveniência
(``sha256`` do arquivo de origem) fica nos metadados do próprio Parquet; o arquivo só é
reconvertido quando o hash ou a versão da especificação mudam.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from quantilica_sql.config import get_settings
from quantilica_sql.core.atomic import file_sha256
from quantilica_sql.lake.specs import SPEC_VERSION, DatasetSpec, get_spec, unique_names

logger = logging.getLogger("quantilica_sql.lake")

DATA_SUFFIXES = {".csv", ".txt", ".zip", ".7z"}
_DATE_SUFFIX = re.compile(r"@[0-9T]+$")
_PARTITION = re.compile(r"^\d{4}(-\d{2})?$")
META_SHA = "quantilica.origin_sha256"
META_SPEC = "quantilica.lake_spec_version"


class LakeError(RuntimeError):
    pass


@dataclass
class ConvertResult:
    source_file: Path
    output: Path
    status: str  # "converted" | "skipped"
    rows: int = 0


@dataclass
class ConvertSummary:
    converted: int = 0
    skipped: int = 0
    rows: int = 0
    errors: list[tuple[Path, str]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def lake_root(explicit: Path | None = None) -> Path:
    return Path(explicit) if explicit else get_settings().lake_dir


def file_key(name: str) -> str:
    """``rais-vinculos_2025-sul@20260513.7z`` → ``rais-vinculos_2025-sul``."""
    stem = name
    for _ in range(2):  # remove .csv/.zip/.7z e depois um eventual .csv interno
        path = Path(stem)
        if path.suffix.lower() in DATA_SUFFIXES:
            stem = path.stem
    return _DATE_SUFFIX.sub("", stem)


def partition_of(path: Path, root: Path | None = None) -> str:
    """Pasta-ancestral que parece ano (``2025``) ou competência (``2026-09``)."""
    parents = path.relative_to(root).parent.parts if root else path.parent.parts
    for part in reversed(parents):
        if _PARTITION.match(part):
            return part
    return ""


def output_path(
    spec: DatasetSpec, src: Path, partition: str, lake: Path | None = None
) -> Path:
    return (
        lake_root(lake)
        / spec.source
        / spec.dataset
        / (partition or "_")
        / f"{file_key(src.name)}.parquet"
    )


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _connect():
    import duckdb

    settings = get_settings()
    conn = duckdb.connect()
    tmp = settings.lake_dir / ".tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    conn.execute(f"SET temp_directory = {_lit(str(tmp))}")
    memory = os.environ.get("QUANTILICA_DUCKDB_MEMORY")
    if memory:
        conn.execute(f"SET memory_limit = {_lit(memory)}")
    threads = os.environ.get("QUANTILICA_DUCKDB_THREADS")
    if threads:
        conn.execute(f"SET threads = {int(threads)}")
    return conn


def origin_matches(output: Path, sha256: str) -> bool:
    """O Parquet existente veio deste hash, com esta versão de especificação."""
    if not output.exists():
        return False
    import duckdb

    try:
        rows = (
            duckdb.connect()
            .execute("SELECT key, value FROM parquet_kv_metadata(?)", [str(output)])
            .fetchall()
        )
    except duckdb.Error:
        return False
    meta = {k.decode(): v.decode() for k, v in rows}
    return meta.get(META_SHA) == sha256 and meta.get(META_SPEC) == str(SPEC_VERSION)


def _materialize(src: Path, tmp: Path) -> list[Path]:
    """Arquivos de texto a ler: o próprio, ou os membros extraídos de ZIP/7z."""
    suffix = src.suffix.lower()
    if suffix in {".csv", ".txt"}:
        return [src]
    if suffix == ".zip":
        with zipfile.ZipFile(src) as z:
            members = [m for m in z.infolist() if not m.is_dir()]
            z.extractall(tmp, members=members)
    elif suffix == ".7z":
        tool = shutil.which("7z") or shutil.which("7zz") or shutil.which("7za")
        if tool is None:
            raise LakeError("7z não encontrado no PATH (necessário para arquivos .7z)")
        proc = subprocess.run(
            [tool, "e", "-y", "-bd", f"-o{tmp}", str(src)],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise LakeError(f"7z falhou em {src.name}: {proc.stderr.strip()[-300:]}")
    else:
        raise LakeError(f"formato não suportado: {src.name}")
    files = sorted(p for p in tmp.rglob("*") if p.is_file())
    if not files:
        raise LakeError(f"{src.name} não contém arquivos")
    return files


def _read_csv(spec: DatasetSpec, files: list[Path]) -> str:
    args = [
        "[" + ", ".join(_lit(str(f)) for f in files) + "]",
        f"delim={_lit(spec.separator)}",
        f"header={'true' if spec.header else 'false'}",
        "all_varchar=true",
        f"encoding={_lit(spec.encoding)}",
        "null_padding=true",
        "strict_mode=false",
    ]
    if not spec.header:
        args.append("names=[" + ", ".join(_lit(n) for n in spec.names) + "]")
    return f"read_csv({', '.join(args)})"


def _select_list(conn, spec: DatasetSpec, read: str) -> str:
    if spec.header:
        header = [
            r[0] for r in conn.execute(f"DESCRIBE SELECT * FROM {read}").fetchall()
        ]
        names = unique_names(header)
        names = [spec.rename.get(n, n) for n in names]
        missing = [c for c in spec.required if c not in names]
        if missing:
            raise LakeError(
                f"cabeçalho inesperado em {spec.source}/{spec.dataset}: faltam "
                f"{missing} (colunas: {names[:6]}…)"
            )
    else:
        header = list(spec.names)
        names = list(spec.names)
    return ", ".join(
        f'"{orig.replace(chr(34), chr(34) * 2)}" AS "{new}"'
        for orig, new in zip(header, names, strict=True)
    )


def convert_file(
    spec: DatasetSpec,
    src: Path,
    *,
    partition: str = "",
    lake: Path | None = None,
    force: bool = False,
) -> ConvertResult:
    """Converte um arquivo de origem em Parquet (atômico e idempotente)."""
    src = Path(src)
    out = output_path(spec, src, partition, lake)
    digest = file_sha256(src)
    if not force and origin_matches(out, digest):
        return ConvertResult(src, out, "skipped")

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = out.with_name(out.name + ".tmp")
    root = lake_root(lake)
    (root / ".tmp").mkdir(parents=True, exist_ok=True)
    conn = _connect()
    try:
        with tempfile.TemporaryDirectory(dir=root / ".tmp") as tmp:
            files = _materialize(src, Path(tmp))
            read = _read_csv(spec, files)
            select = _select_list(conn, spec, read)
            kv = {
                META_SHA: digest,
                META_SPEC: str(SPEC_VERSION),
                "quantilica.source_id": spec.source,
                "quantilica.dataset_id": spec.dataset,
                "quantilica.origin_file": src.name,
                "quantilica.converted_at": datetime.now(UTC).isoformat(
                    timespec="seconds"
                ),
            }
            kv_sql = ", ".join(f"{_lit(k)}: {_lit(v)}" for k, v in kv.items())
            conn.execute(
                f"COPY (SELECT {select} FROM {read}) TO {_lit(str(tmp_out))} "
                f"(FORMAT parquet, COMPRESSION zstd, KV_METADATA {{{kv_sql}}})"
            )
        rows = conn.execute(
            "SELECT sum(num_rows) FROM parquet_file_metadata(?)", [str(tmp_out)]
        ).fetchone()[0]
        os.replace(tmp_out, out)
    except Exception:
        tmp_out.unlink(missing_ok=True)
        raise
    finally:
        conn.close()
    logger.info("lake: %s → %s (%s linhas)", src.name, out, f"{rows:,}")
    return ConvertResult(src, out, "converted", int(rows or 0))


def iter_source_files(root: Path, dataset: str | None = None):
    """(dataset_dir, arquivo) sob ``root``, ignorando sidecars e temporários."""
    for dataset_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        if dataset and dataset_dir.name != dataset:
            continue
        for f in sorted(dataset_dir.rglob("*")):
            if (
                f.is_file()
                and f.suffix.lower() in DATA_SUFFIXES
                and not f.name.endswith((".manifest.json", ".tmp"))
            ):
                yield dataset_dir, f


def convert_tree(
    source: str,
    root: Path,
    *,
    dataset: str | None = None,
    lake: Path | None = None,
    force: bool = False,
) -> ConvertSummary:
    """Converte todos os arquivos de ``root`` (a pasta da fonte, ex.: ``data/pdet``)."""
    summary = ConvertSummary()
    for dataset_dir, f in iter_source_files(Path(root), dataset):
        spec = get_spec(source, dataset_dir.name)
        if spec is None:
            continue
        try:
            res = convert_file(
                spec,
                f,
                partition=partition_of(f, Path(root)),
                lake=lake,
                force=force,
            )
        except Exception as exc:  # um arquivo ruim não derruba os demais
            logger.error("lake: falha em %s: %s", f, exc)
            summary.errors.append((f, str(exc)))
            continue
        if res.status == "skipped":
            summary.skipped += 1
        else:
            summary.converted += 1
            summary.rows += res.rows
    return summary
