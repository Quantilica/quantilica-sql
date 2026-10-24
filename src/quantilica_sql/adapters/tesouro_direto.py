"""Adaptador genérico para dados do Tesouro Direto no PostgreSQL."""

from __future__ import annotations

import csv
import logging
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.core.copy_stream import copy_from_file
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.tesouro_direto")


class TesouroDiretoAdapter(BaseSourceAdapter):
    """Adaptador de carga para vendas, resgates e investidores do Tesouro Direto."""

    @property
    def domain(self) -> str:
        return "tesouro_direto"

    @property
    def schema(self) -> str:
        return "tesouro_direto"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE';
                """,
                (self.schema,),
            )
            tables_count = cur.fetchone()[0]

            files_count = 0
            total_rows = 0
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = 'arquivo_carregado'
                );
                """,
                (self.schema,),
            )
            if cur.fetchone()[0]:
                cur.execute(
                    sql.SQL(
                        "SELECT count(*), coalesce(sum(linhas), 0) "
                        "FROM {}.arquivo_carregado WHERE dataset = 'tesouro_direto';"
                    ).format(sql.Identifier(self.schema))
                )
                res = cur.fetchone()
                files_count, total_rows = res if res else (0, 0)

            return {
                "domain": self.domain,
                "schema": self.schema,
                "tabelas": tables_count,
                "arquivos_carregados": files_count,
                "linhas_totais": total_rows,
            }

    def _derive_table_name(self, filename: str, group_name: str = "") -> str:
        base = filename
        if base.lower().endswith(".csv"):
            base = base[:-4]
        if group_name and not base.startswith(group_name):
            base = f"{group_name}_{base}"
        return self.normalize_identifier(base)

    def load_single_csv(
        self,
        conn: psycopg.Connection,
        file_path: Path,
        group_name: str = "",
        tablespace: str | None = None,
    ) -> int:
        table_name = self._derive_table_name(file_path.name, group_name)

        delimiter = ";"
        encoding = "latin-1"
        col_names: list[str] = []

        for enc in ("utf-8-sig", "utf-8", "latin-1"):
            try:
                with open(file_path, encoding=enc, errors="strict") as f:
                    first_line = f.readline().strip()
                    if ";" in first_line:
                        delimiter = ";"
                    elif "," in first_line:
                        delimiter = ","
                    reader = csv.reader([first_line], delimiter=delimiter)
                    cols = next(reader, [])
                    if cols:
                        col_names = [
                            c.strip().lstrip("\ufeff") for c in cols if c.strip()
                        ]
                        encoding = enc
                        break
            except (UnicodeDecodeError, Exception):
                continue

        if not col_names:
            logger.warning("Arquivo sem colunas válidas: %s", file_path.name)
            return 0

        norm_cols = self.ensure_table_with_schema_evolution(
            conn, table_name, col_names, tablespace=tablespace, add_source_col=True
        )

        rows = copy_from_file(
            conn,
            schema=self.schema,
            table=table_name,
            file_path=file_path,
            columns=norm_cols,
            delimiter=delimiter,
            null_value="",
            encoding="UTF8" if "utf" in encoding.lower() else "LATIN1",
        )

        self.record_and_commit(
            conn,
            filename=file_path.name,
            dataset="tesouro_direto",
            rows_loaded=rows,
            particao=group_name,
            metadata={"tabela": table_name},
        )
        return rows

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        source_dir = self.resolve_source_dir(source_path)
        if not source_dir.exists():
            # Tenta fallback para tddata ou tesouro-direto
            for alt in (
                source_dir.parent / "tesouro-direto",
                source_dir.parent / "tddata",
            ):
                if alt.exists():
                    source_dir = alt
                    break
            else:
                logger.warning("Caminho Tesouro Direto não encontrado: %s", source_dir)
                return 0

        force = kwargs.get("force", False)
        tablespace = kwargs.get("tablespace")
        staging_dir = kwargs.get("staging_dir")

        if source_dir.is_file():
            is_staged = False
            local_file = source_dir
            try:
                if staging_dir:
                    local_file = self.stage_file_locally(source_dir, staging_dir)
                    is_staged = True
                return self.load_single_csv(conn, local_file, tablespace=tablespace)
            finally:
                if is_staged and local_file.exists():
                    local_file.unlink(missing_ok=True)

        theme_filter = kwargs.get("dataset") or kwargs.get("table")
        all_files: list[tuple[str, Path]] = []

        for item in source_dir.iterdir():
            if item.is_dir():
                group_name = item.name
                if theme_filter and theme_filter not in group_name:
                    continue
                for f in item.iterdir():
                    if f.is_file() and f.suffix.lower() == ".csv":
                        all_files.append((group_name, f))
            elif item.is_file() and item.suffix.lower() == ".csv":
                all_files.append(("", item))

        all_files.sort(key=lambda x: x[1].name)
        total_rows = 0
        total_loaded = 0
        t0_all = time.time()

        logger.info(
            "Iniciando carga de %d arquivos Tesouro Direto a partir de %s",
            len(all_files),
            source_dir,
        )

        for group_name, fpath in all_files:
            if not force and self.is_file_already_loaded(
                conn, fpath.name, dataset="tesouro_direto", particao=group_name
            ):
                logger.info("  PULANDO %s (já carregado)", fpath.name)
                continue

            t_file = time.time()
            is_staged = False
            local_file = fpath
            try:
                if staging_dir:
                    local_file = self.stage_file_locally(fpath, staging_dir)
                    is_staged = True

                rows = self.load_single_csv(
                    conn, local_file, group_name=group_name, tablespace=tablespace
                )
                total_rows += rows
                total_loaded += 1
                logger.info(
                    "  [tesouro_direto] %s: %s linhas em %.2fs",
                    fpath.name,
                    f"{rows:,}",
                    time.time() - t_file,
                )
            except Exception as exc:
                logger.error("Erro ao carregar %s: %s", fpath.name, exc)
                if not conn.autocommit:
                    conn.rollback()
                raise
            finally:
                if is_staged and local_file.exists():
                    local_file.unlink(missing_ok=True)

        logger.info(
            "Carga Tesouro Direto concluída: %d arquivos, %s linhas em %.2fs",
            total_loaded,
            f"{total_rows:,}",
            time.time() - t0_all,
        )
        return total_rows
