"""Adaptador genérico para dados fiscais do Resultado do Tesouro Nacional (RTN)."""

from __future__ import annotations

import csv
import logging
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.core.atomic import file_sha256
from quantilica_sql.core.copy_stream import copy_from_file
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.rtn")


class RtnAdapter(BaseSourceAdapter):
    """Adaptador de carga para séries fiscais e demonstrativos do RTN no PostgreSQL."""

    @property
    def domain(self) -> str:
        return "rtn"

    @property
    def schema(self) -> str:
        return "rtn"

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
                        "FROM {}.arquivo_carregado WHERE dataset = 'rtn';"
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

    def _derive_table_name(self, filename: str) -> str:
        base = filename
        if base.lower().endswith(".csv"):
            base = base[:-4]
        return self.normalize_identifier(f"demonstrativo_{base}")

    def load_single_csv(
        self,
        conn: psycopg.Connection,
        file_path: Path,
        competencia: str = "",
        tablespace: str | None = None,
        sha256: str | None = None,
        source_name: str | None = None,
    ) -> int:
        table_name = self._derive_table_name(file_path.name)

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

        filename = source_name or file_path.name
        with self.atomic_file(
            conn,
            table=table_name,
            filename=filename,
            dataset="rtn",
            particao=competencia,
            sha256=sha256,
        ):
            rows = copy_from_file(
                conn,
                schema=self.schema,
                table=table_name,
                file_path=file_path,
                columns=norm_cols,
                delimiter=delimiter,
                null_value="",
                encoding="UTF8" if "utf" in encoding.lower() else "LATIN1",
                header=True,
            )

            self.record_and_commit(
                conn,
                filename=filename,
                dataset="rtn",
                rows_loaded=rows,
                particao=competencia,
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
            logger.warning("Caminho RTN não encontrado: %s", source_dir)
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
                return self.load_single_csv(
                    conn,
                    local_file,
                    tablespace=tablespace,
                    sha256=file_sha256(source_dir),
                    source_name=source_dir.name,
                )
            finally:
                if is_staged and local_file.exists():
                    local_file.unlink(missing_ok=True)

        comp_filter = kwargs.get("competencia")
        all_files: list[tuple[str, Path]] = []

        for item in source_dir.iterdir():
            if item.is_dir():
                comp = item.name
                if comp_filter and comp_filter not in comp:
                    continue
                for f in item.iterdir():
                    if f.is_file() and f.suffix.lower() == ".csv":
                        all_files.append((comp, f))
            elif item.is_file() and item.suffix.lower() == ".csv":
                all_files.append(("", item))

        all_files.sort(key=lambda x: x[1].name)
        total_rows = 0
        total_loaded = 0
        t0_all = time.time()

        logger.info(
            "Iniciando carga de %d arquivos RTN a partir de %s",
            len(all_files),
            source_dir,
        )

        for comp, fpath in all_files:
            do_load, digest = self.should_load_file(
                conn, fpath, dataset="rtn", particao=comp, force=force
            )
            if not do_load:
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
                    conn,
                    local_file,
                    competencia=comp,
                    tablespace=tablespace,
                    sha256=digest,
                    source_name=fpath.name,
                )
                total_rows += rows
                total_loaded += 1
                logger.info(
                    "  [rtn] %s: %s linhas em %.2fs",
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
            "Carga RTN concluída: %d arquivos, %s linhas em %.2fs",
            total_loaded,
            f"{total_rows:,}",
            time.time() - t0_all,
        )
        return total_rows
