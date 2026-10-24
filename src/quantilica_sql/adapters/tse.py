"""Adaptador genérico para dados eleitorais do TSE no PostgreSQL."""

from __future__ import annotations

import csv
import logging
import re
import time
import zipfile
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.core.copy_stream import copy_from_file
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.tse")


class TseAdapter(BaseSourceAdapter):
    """Adaptador de carga para dados eleitorais e de candidatos do TSE."""

    @property
    def domain(self) -> str:
        return "tse"

    @property
    def schema(self) -> str:
        return "tse"

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
                        "FROM {}.arquivo_carregado WHERE dataset = 'tse';"
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
        if base.lower().endswith(".csv") or base.lower().endswith(".zip"):
            base = base[:-4]
        # Remove sufixos como _BRASIL, _SP, etc.
        base = re.sub(r"_[A-Z]{2}$", "", base)
        base = re.sub(r"_brasil$", "", base, flags=re.IGNORECASE)
        # Remove ano _YYYY
        base = re.sub(r"_\d{4}$", "", base)
        return self.normalize_identifier(base)

    def load_single_csv(
        self,
        conn: psycopg.Connection,
        file_path: Path,
        particao: str = "",
        tablespace: str | None = None,
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
            dataset="tse",
            rows_loaded=rows,
            particao=particao,
            metadata={"tabela": table_name},
        )
        return rows

    def load_single_zip(
        self,
        conn: psycopg.Connection,
        file_path: Path,
        particao: str = "",
        tablespace: str | None = None,
    ) -> int:
        total_rows = 0
        with zipfile.ZipFile(file_path) as z:
            csv_members = [
                m
                for m in z.namelist()
                if m.lower().endswith(".csv") and not m.startswith("__MACOSX")
            ]
            for member in csv_members:
                table_name = self._derive_table_name(Path(member).name)
                with z.open(member) as f:
                    header_line = (
                        f.readline().decode("latin-1", errors="replace").strip()
                    )
                    delim = ";" if ";" in header_line else ","
                    col_names = [
                        c.strip() for c in header_line.split(delim) if c.strip()
                    ]
                    if not col_names:
                        continue
                    norm_cols = self.ensure_table_with_schema_evolution(
                        conn,
                        table_name,
                        col_names,
                        tablespace=tablespace,
                        add_source_col=True,
                    )

                with z.open(member) as f:
                    cols_sql = sql.SQL(", ").join(sql.Identifier(c) for c in norm_cols)
                    copy_sql = sql.SQL(
                        """
                        COPY {}.{} ({})
                        FROM STDIN WITH (
                            FORMAT csv,
                            DELIMITER {},
                            HEADER true,
                            ENCODING 'LATIN1',
                            NULL ''
                        )
                        """
                    ).format(
                        sql.Identifier(self.schema),
                        sql.Identifier(table_name),
                        cols_sql,
                        sql.Literal(delim),
                    )
                    with conn.cursor() as cur:
                        with cur.copy(copy_sql) as copy:
                            while chunk := f.read(131072):
                                copy.write(chunk)
                        m_rows = (
                            cur.rowcount if cur.rowcount and cur.rowcount >= 0 else 0
                        )
                        total_rows += m_rows

        self.record_and_commit(
            conn,
            filename=file_path.name,
            dataset="tse",
            rows_loaded=total_rows,
            particao=particao,
            metadata={"membros": len(csv_members)},
        )
        return total_rows

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        source_dir = self.resolve_source_dir(source_path)
        if not source_dir.exists():
            logger.warning("Caminho TSE não encontrado: %s", source_dir)
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
                if local_file.suffix.lower() == ".zip":
                    return self.load_single_zip(conn, local_file, tablespace=tablespace)
                return self.load_single_csv(conn, local_file, tablespace=tablespace)
            finally:
                if is_staged and local_file.exists():
                    local_file.unlink(missing_ok=True)

        all_files = sorted(
            list(source_dir.glob("*.zip")) + list(source_dir.glob("*.csv")),
            key=lambda x: x.name,
        )
        total_rows = 0
        total_loaded = 0
        t0_all = time.time()

        logger.info(
            "Iniciando carga de %d arquivos TSE a partir de %s",
            len(all_files),
            source_dir,
        )

        for fpath in all_files:
            if not force and self.is_file_already_loaded(
                conn, fpath.name, dataset="tse"
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

                if local_file.suffix.lower() == ".zip":
                    rows = self.load_single_zip(conn, local_file, tablespace=tablespace)
                else:
                    rows = self.load_single_csv(conn, local_file, tablespace=tablespace)

                total_rows += rows
                total_loaded += 1
                logger.info(
                    "  [tse] %s: %s linhas em %.2fs",
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
            "Carga TSE concluída: %d arquivos, %s linhas em %.2fs",
            total_loaded,
            f"{total_rows:,}",
            time.time() - t0_all,
        )
        return total_rows
