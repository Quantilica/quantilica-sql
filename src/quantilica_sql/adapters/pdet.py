"""Adaptador genérico para microdados trabalhistas do PDET (CAGED e RAIS)."""

from __future__ import annotations

import logging
import re
import tempfile
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.core.copy_stream import copy_from_file, copy_from_rows
from quantilica_sql.core.tracker import (
    ensure_tracker_table,
    is_file_loaded,
    record_loaded_file,
)
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.pdet")


def _get_table_columns(conn: psycopg.Connection, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]


def _ensure_table(
    conn: psycopg.Connection, schema: str, table: str, columns: list[str]
) -> None:
    ensure_schema(conn, schema)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
            """,
            (schema, table),
        )
        exists = cur.fetchone()[0]
        if not exists and columns:
            cols_sql = ", ".join(f'"{c}" TEXT' for c in columns)
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({cols_sql});'
            )
    if not conn.autocommit:
        conn.commit()


class PdetAdapter(BaseSourceAdapter):
    """Adaptador de carga para microdados da RAIS e CAGED."""

    @property
    def domain(self) -> str:
        return "pdet"

    @property
    def schema(self) -> str:
        return "pdet"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        ensure_tracker_table(conn, self.schema)
        with conn.cursor() as cur:
            query = sql.SQL(
                """
                SELECT dataset, count(*), coalesce(sum(linhas), 0)
                FROM {schema}.{table}
                GROUP BY dataset
                ORDER BY sum(linhas) DESC;
                """
            ).format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier("arquivo_carregado"),
            )
            cur.execute(query)
            rows = cur.fetchall()
            return {
                "domain": self.domain,
                "schema": self.schema,
                "datasets": {
                    r[0]: {"arquivos": r[1], "linhas": int(r[2])} for r in rows
                },
            }

    def _load_single_item(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        dataset = kwargs.get("dataset", "caged")
        particao = str(
            kwargs.get("particao") or kwargs.get("year") or kwargs.get("date") or ""
        )
        filename = kwargs.get("filename") or (
            source_path.name if source_path else f"{dataset}_{particao}.csv"
        )
        force = kwargs.get("force", False)

        if not force and is_file_loaded(
            conn,
            schema=self.schema,
            filename=filename,
            dataset=dataset,
            particao=particao,
        ):
            logger.info(
                "PDET %s/%s (%s) já registrado em %s.arquivo_carregado. Pulando.",
                dataset,
                particao,
                filename,
                self.schema,
            )
            return 0

        # Identificação da tabela de destino
        table = kwargs.get("table")
        if not table:
            if "estabelecimentos" in dataset:
                table = "rais_estabelecimentos"
            elif "vinculos" in dataset or "rais" in dataset:
                table = "rais_vinculos"
            else:
                table = "caged"

        columns = kwargs.get("columns")
        df = kwargs.get("df")
        rows_loaded = 0

        if df is not None:
            import polars as pl

            if columns is None:
                existing_cols = _get_table_columns(conn, self.schema, table)
                columns = existing_cols if existing_cols else list(df.columns)
            _ensure_table(conn, self.schema, table, columns)

            staging_dir = kwargs.get("staging_dir")
            with tempfile.TemporaryDirectory(
                prefix="pdet_stream_", dir=staging_dir
            ) as tmp_dir:
                tsv_path = Path(tmp_dir) / "stream_copy.tsv"
                select_exprs = [
                    pl.col(c) if c in df.columns else pl.lit(None).alias(c)
                    for c in columns
                ]
                subset = df.select(select_exprs)
                n_rows = len(subset)
                subset.write_csv(
                    tsv_path, separator="\t", include_header=False, null_value=""
                )
                del subset

                rows_loaded = copy_from_file(
                    conn,
                    schema=self.schema,
                    table=table,
                    file_path=tsv_path,
                    columns=columns,
                    delimiter="\t",
                    null_value="",
                )
                if rows_loaded == 0 and n_rows > 0:
                    rows_loaded = n_rows
        elif source_path is not None and source_path.exists():
            if columns is None:
                columns = _get_table_columns(conn, self.schema, table)
            if columns:
                _ensure_table(conn, self.schema, table, columns)
            delimiter = kwargs.get(
                "delimiter", "\t" if source_path.suffix in (".tsv", ".txt") else ";"
            )
            null_val = kwargs.get("null_value", "")
            rows_loaded = copy_from_file(
                conn,
                schema=self.schema,
                table=table,
                file_path=source_path,
                columns=columns,
                delimiter=delimiter,
                null_value=null_val,
            )
        elif kwargs.get("rows"):
            rows = kwargs.get("rows")
            if columns is None:
                columns = _get_table_columns(conn, self.schema, table)
            if columns:
                _ensure_table(conn, self.schema, table, columns)
            rows_loaded = copy_from_rows(
                conn,
                schema=self.schema,
                table=table,
                columns=columns or [],
                rows=rows,
            )

        record_loaded_file(
            conn,
            schema=self.schema,
            filename=filename,
            dataset=dataset,
            rows_loaded=rows_loaded,
            particao=particao,
            metadata={"tabela": f"{self.schema}.{table}"},
        )
        if not conn.autocommit:
            conn.commit()
        return rows_loaded

    def _load_directory(
        self,
        conn: psycopg.Connection,
        source_dir: Path,
        **kwargs: Any,
    ) -> int:
        if not source_dir.exists():
            logger.warning("Diretório PDET não encontrado: %s", source_dir)
            return 0

        force = kwargs.get("force", False)
        staging_dir = kwargs.get("staging_dir")
        dataset_filter = kwargs.get("dataset")
        total_rows = 0

        archives: list[Path] = []
        for ext in ("*.7z", "*.zip", "*.csv", "*.txt"):
            archives.extend(source_dir.rglob(ext))

        archives.sort(key=lambda x: x.name)
        logger.info(
            "Encontrados %d arquivos PDET para processamento em %s",
            len(archives),
            source_dir,
        )

        for arch in archives:
            dataset_name = arch.parent.name
            if dataset_filter and dataset_filter not in dataset_name:
                continue

            year_val = ""
            for part in list(arch.parts) + [arch.stem]:
                digits = re.findall(r"\b(19\d\d|20\d\d)\b", part)
                if digits:
                    year_val = digits[-1]
                    break

            if not force and self.is_file_already_loaded(
                conn, arch.name, dataset=dataset_name, particao=year_val
            ):
                logger.info("  PULANDO %s (já carregado)", arch.name)
                continue

            t_file = time.time()
            is_staged = False
            local_file = arch
            try:
                if staging_dir:
                    local_file = self.stage_file_locally(arch, staging_dir)
                    is_staged = True

                rows = self._load_single_item(
                    conn,
                    source_path=local_file,
                    filename=arch.name,
                    dataset=dataset_name,
                    particao=year_val,
                    **kwargs,
                )
                total_rows += rows
                logger.info(
                    "  [pdet] %s: %s linhas em %.2fs",
                    arch.name,
                    f"{rows:,}",
                    time.time() - t_file,
                )
            except Exception as exc:
                logger.error("Erro ao carregar %s: %s", arch.name, exc)
                if not conn.autocommit:
                    conn.rollback()
                raise
            finally:
                if is_staged and local_file.exists():
                    local_file.unlink(missing_ok=True)

        return total_rows

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        if source_path is None and "df" not in kwargs and "rows" not in kwargs:
            source_path = self.resolve_source_dir()

        if source_path is not None and Path(source_path).is_dir():
            return self._load_directory(conn, Path(source_path), **kwargs)

        return self._load_single_item(conn, source_path, **kwargs)
