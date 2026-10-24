"""Adaptador para dados abertos cadastrais de CNPJs da RFB."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any

import polars as pl
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

logger = logging.getLogger("quantilica_sql.adapters.rfb")

COLUMN_RENAMES: dict[str, dict[str, str]] = {
    "estabelecimentos": {
        "ddd1": "ddd_1",
        "telefone1": "telefone_1",
        "ddd2": "ddd_2",
        "telefone2": "telefone_2",
    },
    "empresas": {
        "ente_federativo_responsavel": "ente_federativo",
    },
    "socios": {
        "nome_representante_legal": "nome_representante",
        "qualificacao_representante_legal": "qualificacao_representante",
    },
}

TABLE_SCHEMAS: dict[str, list[str]] = {
    "cnaes": ["codigo", "descricao"],
    "motivos": ["codigo", "descricao"],
    "municipios": ["codigo", "descricao"],
    "naturezas": ["codigo", "descricao"],
    "paises": ["codigo", "descricao"],
    "qualificacoes": ["codigo", "descricao"],
    "simples": [
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ],
    "socios": [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante",
        "faixa_etaria",
    ],
    "empresas": [
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo",
    ],
    "estabelecimentos": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ],
}


class RfbAdapter(BaseSourceAdapter):
    """Adaptador de carga para dados de CNPJs da Receita Federal."""

    @property
    def domain(self) -> str:
        return "rfb"

    @property
    def schema(self) -> str:
        return "rfb_cnpj"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        ensure_tracker_table(conn, self.schema)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s;
                """,
                (self.schema,),
            )
            tables_count = cur.fetchone()[0]

            query = sql.SQL(
                """
                SELECT count(*), coalesce(sum(linhas), 0)
                FROM {schema}.{table};
                """
            ).format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier("arquivo_carregado"),
            )
            cur.execute(query)
            res = cur.fetchone()
            files_count, total_rows = res if res else (0, 0)

            return {
                "domain": self.domain,
                "schema": self.schema,
                "tabelas_particionadas": tables_count,
                "arquivos_carregados": files_count,
                "linhas_totais": total_rows,
            }

    def ensure_root_tables(
        self, conn: psycopg.Connection, tablespace: str = ""
    ) -> None:
        """Cria schema rfb_cnpj, tabela de controle e particionadas principais."""
        ensure_schema(conn, self.schema)
        ensure_tracker_table(conn, self.schema)
        tb_clause = f" TABLESPACE {tablespace}" if tablespace else ""

        with conn.cursor() as cur:
            for tbl, cols in TABLE_SCHEMAS.items():
                cols_def = [f'"{c}" TEXT' for c in cols]
                cols_def.append('"competencia" VARCHAR(7) NOT NULL')
                ddl = f"""
                    CREATE TABLE IF NOT EXISTS {self.schema}.{tbl} (
                        {", ".join(cols_def)}
                    ) PARTITION BY LIST (competencia){tb_clause};
                """
                cur.execute(ddl)
        if not conn.autocommit:
            conn.commit()

    def ensure_partition(
        self,
        conn: psycopg.Connection,
        tbl: str,
        competencia: str,
        tablespace: str = "",
    ) -> str:
        """Garante a partição da tabela para a competência na tablespace."""
        self.ensure_root_tables(conn, tablespace)
        part_suffix = competencia.replace("-", "_")
        part_name = f"{tbl}_{part_suffix}"
        tb_clause = f" TABLESPACE {tablespace}" if tablespace else ""

        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {self.schema}.{part_name} "
                f"PARTITION OF {self.schema}.{tbl} "
                f"FOR VALUES IN ('{competencia}'){tb_clause};"
            )
            cur.execute(
                f"ALTER TABLE {self.schema}.{part_name} "
                f"ALTER COLUMN competencia SET DEFAULT '{competencia}';"
            )
        if not conn.autocommit:
            conn.commit()
        return part_name

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        competencia = kwargs.get("competencia", "2026-10")
        table_name = kwargs.get("table_name") or kwargs.get("table")
        if not table_name:
            if source_path is not None:
                lower_stem = source_path.stem.lower()
                for known_tbl in TABLE_SCHEMAS:
                    if known_tbl in lower_stem:
                        table_name = known_tbl
                        break
            if not table_name:
                table_name = "estabelecimentos"

        filename = kwargs.get("filename") or (
            source_path.name if source_path else f"{table_name}_{competencia}.csv"
        )
        dataset = kwargs.get("dataset", "rfb_cnpj")
        force = kwargs.get("force", False)

        if not force and is_file_loaded(
            conn,
            schema=self.schema,
            filename=filename,
            dataset=dataset,
            particao=competencia,
        ):
            logger.info(
                "RFB CNPJ %s/%s (%s) já registrado em %s.arquivo_carregado. Pulando.",
                table_name,
                competencia,
                filename,
                self.schema,
            )
            return 0

        tablespace = kwargs.get("tablespace", "")
        part_name = self.ensure_partition(
            conn, table_name, competencia, tablespace=tablespace
        )
        cols = TABLE_SCHEMAS.get(table_name, [])

        df = kwargs.get("df")
        rows_loaded = 0

        if df is not None:
            if table_name in COLUMN_RENAMES:
                renames = {
                    k: v
                    for k, v in COLUMN_RENAMES[table_name].items()
                    if k in df.columns
                }
                if renames:
                    df = df.rename(renames)
            select_cols = [c for c in cols if c in df.columns]
            if not select_cols:
                select_cols = list(df.columns)
            subset = df.select(select_cols)
            n_rows = len(subset)

            staging_dir = kwargs.get("staging_dir")
            with tempfile.TemporaryDirectory(
                prefix="rfb_stream_", dir=staging_dir
            ) as tmp_dir:
                tmp_csv = Path(tmp_dir) / f"{part_name}_stream.csv"
                subset.write_csv(
                    tmp_csv, separator=";", include_header=False, null_value=""
                )
                del subset

                rows_loaded = copy_from_file(
                    conn,
                    schema=self.schema,
                    table=part_name,
                    file_path=tmp_csv,
                    columns=select_cols,
                    delimiter=";",
                    null_value="",
                )
                if rows_loaded == 0 and n_rows > 0:
                    rows_loaded = n_rows
        elif source_path is not None and source_path.exists():
            if source_path.suffix.lower() == ".zip":
                try:
                    from rfb_cnpj_fetcher.reader import LAYOUTS, _read_text_frame

                    frame = _read_text_frame(source_path, LAYOUTS[table_name])
                    frame = frame.with_columns(
                        pl.col(pl.String).str.replace_all("\x00", "")
                    )
                    if table_name in COLUMN_RENAMES:
                        renames = {
                            k: v
                            for k, v in COLUMN_RENAMES[table_name].items()
                            if k in frame.columns
                        }
                        if renames:
                            frame = frame.rename(renames)
                    frame = frame.select(cols)
                    n_rows = len(frame)

                    staging_dir = kwargs.get("staging_dir")
                    with tempfile.TemporaryDirectory(
                        prefix="rfb_zip_stream_", dir=staging_dir
                    ) as tmp_dir:
                        tmp_csv = Path(tmp_dir) / f"{part_name}_{source_path.stem}.csv"
                        frame.write_csv(
                            tmp_csv, separator=";", include_header=False, null_value=""
                        )
                        del frame

                        rows_loaded = copy_from_file(
                            conn,
                            schema=self.schema,
                            table=part_name,
                            file_path=tmp_csv,
                            columns=cols,
                            delimiter=";",
                            null_value="",
                        )
                        if rows_loaded == 0 and n_rows > 0:
                            rows_loaded = n_rows
                except ImportError:
                    logger.warning(
                        "rfb_cnpj_fetcher não disponível para extração direta de ZIP."
                    )
            else:
                rows_loaded = copy_from_file(
                    conn,
                    schema=self.schema,
                    table=part_name,
                    file_path=source_path,
                    columns=cols,
                    delimiter=kwargs.get("delimiter", ";"),
                    null_value=kwargs.get("null_value", ""),
                )
        elif kwargs.get("rows"):
            rows = kwargs.get("rows")
            rows_loaded = copy_from_rows(
                conn,
                schema=self.schema,
                table=part_name,
                columns=cols,
                rows=rows,
            )

        record_loaded_file(
            conn,
            schema=self.schema,
            filename=filename,
            dataset=dataset,
            rows_loaded=rows_loaded,
            particao=competencia,
            metadata={"tabela": table_name},
        )
        return rows_loaded
