"""Adaptador genérico de carga para dados abertos da CVM no PostgreSQL."""

from __future__ import annotations

import io
import logging
import re
import time
import zipfile
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.cvm")


def derive_table_name(member_or_file_name: str) -> str:
    """Deriva nome canônico da tabela CVM a partir do arquivo CSV interno ou externo.

    Exemplos:
      dfp_cia_aberta_2026.csv -> dfp_cia_aberta
      dfp_cia_aberta_BPA_con_2026.csv -> dfp_cia_aberta_bpa_con
      cad_cia_aberta@20261017.csv -> cad_cia_aberta
      inf_diario_fi_202610.csv -> inf_diario_fi
    """
    base = member_or_file_name
    if base.lower().endswith(".csv"):
        base = base[:-4]
    base = re.sub(r"@\d+.*$", "", base)
    base = re.sub(r"_\d{4}(-\d{2})?$", "", base)
    base = re.sub(r"_\d{6}$", "", base)
    clean = base.strip().lower()
    clean = re.sub(r"[^\w]+", "_", clean)
    clean = clean.strip("_")
    return clean or "cvm_dados"


class CvmAdapter(BaseSourceAdapter):
    """Adaptador de carga de alto desempenho para dados da CVM."""

    @property
    def domain(self) -> str:
        return "cvm"

    @property
    def schema(self) -> str:
        return "cvm"

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
                        "FROM {}.arquivo_carregado WHERE dataset = 'cvm';"
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

    def _copy_stream_to_table(
        self,
        conn: psycopg.Connection,
        table_name: str,
        col_names: list[str],
        stream: io.BufferedIOBase | Any,
    ) -> int:
        quoted_cols = sql.SQL(", ").join(sql.Identifier(c) for c in col_names)
        copy_sql = sql.SQL(
            """
            COPY {}.{} ({})
            FROM STDIN WITH (
                FORMAT csv,
                DELIMITER ';',
                HEADER true,
                ENCODING 'LATIN1',
                QUOTE E'\b',
                NULL ''
            )
            """
        ).format(
            sql.Identifier(self.schema),
            sql.Identifier(table_name),
            quoted_cols,
        )
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                while chunk := stream.read(131072):  # buffer 128KB
                    copy.write(chunk)
            rows_copied = cur.rowcount if cur.rowcount and cur.rowcount >= 0 else 0
        return rows_copied

    def load_single_csv(
        self,
        conn: psycopg.Connection,
        file_path: Path,
        group_name: str = "",
        tablespace: str | None = None,
    ) -> int:
        table_name = derive_table_name(file_path.name)
        with open(file_path, "rb") as f:
            header_line = f.readline().decode("latin-1", errors="replace").strip()
            col_names = [c.strip() for c in header_line.split(";") if c.strip()]
            if not col_names:
                logger.warning("Arquivo vazio ou sem cabeçalho: %s", file_path.name)
                return 0
            norm_cols = self.ensure_table_with_schema_evolution(
                conn, table_name, col_names, tablespace=tablespace, add_source_col=True
            )
            f.seek(0)
            rows = self._copy_stream_to_table(conn, table_name, norm_cols, f)

        self.record_and_commit(
            conn,
            filename=file_path.name,
            dataset="cvm",
            rows_loaded=rows,
            particao=group_name,
            metadata={"tabela": table_name},
        )
        return rows

    def load_single_zip(
        self,
        conn: psycopg.Connection,
        file_path: Path,
        group_name: str = "",
        tablespace: str | None = None,
    ) -> int:
        total_rows = 0
        with zipfile.ZipFile(file_path) as z:
            csv_members = [m for m in z.namelist() if m.lower().endswith(".csv")]
            for member in csv_members:
                table_name = derive_table_name(member)
                with z.open(member) as f:
                    header_line = (
                        f.readline().decode("latin-1", errors="replace").strip()
                    )
                    col_names = [c.strip() for c in header_line.split(";") if c.strip()]
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
                    member_rows = self._copy_stream_to_table(
                        conn, table_name, norm_cols, f
                    )
                    total_rows += member_rows

        self.record_and_commit(
            conn,
            filename=file_path.name,
            dataset="cvm",
            rows_loaded=total_rows,
            particao=group_name,
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
            logger.warning("Caminho CVM não encontrado: %s", source_dir)
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
                ext = local_file.suffix.lower()
                if ext == ".zip":
                    return self.load_single_zip(conn, local_file, tablespace=tablespace)
                elif ext == ".csv":
                    return self.load_single_csv(conn, local_file, tablespace=tablespace)
                else:
                    logger.warning("Formato não suportado: %s", local_file.name)
                    return 0
            finally:
                if is_staged and local_file.exists():
                    local_file.unlink(missing_ok=True)

        dataset_filter = kwargs.get("dataset") or kwargs.get("table")
        all_files: list[tuple[str, Path]] = []

        for item in source_dir.iterdir():
            if item.is_dir():
                group_name = item.name
                if dataset_filter and dataset_filter not in group_name:
                    continue
                for f in item.iterdir():
                    if f.is_file() and f.suffix.lower() in (".csv", ".zip"):
                        all_files.append((group_name, f))
            elif item.is_file() and item.suffix.lower() in (".csv", ".zip"):
                all_files.append(("", item))

        all_files.sort(key=lambda x: x[1].name)
        total_rows = 0
        total_loaded = 0
        t0_all = time.time()

        logger.info(
            "Iniciando carga de %d arquivos CVM a partir de %s",
            len(all_files),
            source_dir,
        )

        for group_name, fpath in all_files:
            if not force and self.is_file_already_loaded(
                conn, fpath.name, dataset="cvm", particao=group_name
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
                    rows = self.load_single_zip(
                        conn, local_file, group_name=group_name, tablespace=tablespace
                    )
                else:
                    rows = self.load_single_csv(
                        conn, local_file, group_name=group_name, tablespace=tablespace
                    )

                total_rows += rows
                total_loaded += 1
                logger.info(
                    "  [cvm] %s: %s linhas em %.2fs",
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
            "Carga CVM concluída: %d arquivos, %s linhas em %.2fs",
            total_loaded,
            f"{total_rows:,}",
            time.time() - t0_all,
        )
        return total_rows
