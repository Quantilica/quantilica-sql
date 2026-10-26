"""Adaptador para dados abertos cadastrais de CNPJs da RFB."""

from __future__ import annotations

import logging
import re
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.config import get_settings
from quantilica_sql.core.copy_stream import copy_from_file, copy_from_rows
from quantilica_sql.core.tracker import (
    ensure_tracker_table,
    is_file_loaded,
    record_loaded_file,
)
from quantilica_sql.core.txscope import commit_if_standalone
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

TABLE_ORDER: list[str] = [
    "cnaes",
    "motivos",
    "municipios",
    "naturezas",
    "paises",
    "qualificacoes",
    "simples",
    "socios",
    "empresas",
    "estabelecimentos",
]


def stage_file_locally(src: Path, staging_dir: Path, max_retries: int = 10) -> Path:
    """Copia arquivo para SSD local com retomada incremental contra stalls de rede."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    dst = staging_dir / f"stage_{src.name}"

    expected_size = src.stat().st_size

    if dst.exists() and dst.stat().st_size == expected_size:
        if dst.suffix.lower() == ".zip":
            if zipfile.is_zipfile(dst):
                return dst
            dst.unlink(missing_ok=True)
        else:
            return dst

    for attempt in range(1, max_retries + 1):
        try:
            current_size = dst.stat().st_size if dst.exists() else 0
            if current_size > expected_size:
                dst.unlink(missing_ok=True)
                current_size = 0

            with open(src, "rb") as f_in, open(dst, "ab") as f_out:
                if current_size > 0:
                    f_in.seek(current_size)
                while chunk := f_in.read(1048576 * 2):  # buffer de 2MB
                    f_out.write(chunk)

            if not dst.exists() or dst.stat().st_size != expected_size:
                actual = dst.stat().st_size if dst.exists() else 0
                raise OSError(
                    f"Tamanho copiado ({actual}) difere do esperado ({expected_size})"
                )

            if dst.suffix.lower() == ".zip" and not zipfile.is_zipfile(dst):
                dst.unlink(missing_ok=True)
                raise zipfile.BadZipFile(
                    f"Arquivo staged {dst.name} não é um ZIP íntegro."
                )

            return dst
        except Exception as exc:
            if attempt == max_retries:
                if dst.exists():
                    dst.unlink(missing_ok=True)
                raise
            delay = min(2**attempt, 15)
            logger.warning(
                "Falha no staging de %s (tentativa %d/%d): %s. Aguardando %ds...",
                src.name,
                attempt,
                max_retries,
                exc,
                delay,
            )
            time.sleep(delay)
    return dst


class RfbAdapter(BaseSourceAdapter):
    """Adaptador de carga para dados de CNPJs da Receita Federal."""

    deprecated = (
        "Os microdados desta fonte saem do PostgreSQL e passam a viver em"
        " Parquet (extra 'lake'): use `quantilica sql lake convert rfb-"
        "cnpj`. Este adaptador está congelado."
    )

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
        commit_if_standalone(conn)

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
        commit_if_standalone(conn)
        return part_name

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        if source_path is None and "df" not in kwargs and "rows" not in kwargs:
            default_path = self.resolve_source_dir()
            if not default_path.exists():
                alt = default_path.parent / "rfb-cnpj"
                if alt.exists():
                    default_path = alt
            if default_path.exists():
                source_path = default_path

        if source_path is not None and source_path.is_dir():
            return self._load_directory(conn, source_path, **kwargs)

        return self._load_single_item(conn, source_path, **kwargs)

    def _load_single_item(
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
                    import polars as pl
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
                    logger.info(
                        "rfb_cnpj_fetcher ausente; usando transcodificação nativa."
                    )
                    import io

                    staging_dir = kwargs.get("staging_dir")
                    with tempfile.TemporaryDirectory(
                        prefix="rfb_zip_std_", dir=staging_dir
                    ) as tmp_dir:
                        tmp_csv = Path(tmp_dir) / f"{part_name}_{source_path.stem}.csv"
                        with zipfile.ZipFile(source_path) as z:
                            members = [m for m in z.namelist() if not m.endswith("/")]
                            if members:
                                with (
                                    z.open(members[0]) as raw,
                                    open(
                                        tmp_csv, "w", encoding="utf-8", newline=""
                                    ) as out,
                                ):
                                    text_in = io.TextIOWrapper(
                                        raw, encoding="latin-1", newline=""
                                    )
                                    for line in text_in:
                                        out.write(line.replace("\x00", ""))

                        if tmp_csv.exists() and tmp_csv.stat().st_size > 0:
                            rows_loaded = copy_from_file(
                                conn,
                                schema=self.schema,
                                table=part_name,
                                file_path=tmp_csv,
                                columns=cols,
                                delimiter=";",
                                null_value="",
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
        commit_if_standalone(conn)
        return rows_loaded

    def _load_directory(
        self,
        conn: psycopg.Connection,
        source_dir: Path,
        **kwargs: Any,
    ) -> int:
        """Processa competências e arquivos ZIP de um diretório da RFB."""
        settings = get_settings()
        tablespace = kwargs.get("tablespace") or settings.tablespace_rfb
        if tablespace == "pg_default":
            tablespace = ""
        force = kwargs.get("force", False)

        staging_dir_opt = kwargs.get("staging_dir")
        staging_dir = (
            Path(staging_dir_opt)
            if staging_dir_opt
            else (settings.staging_dir / "rfb_staging")
        )
        staging_dir.mkdir(parents=True, exist_ok=True)

        comp_dirs: list[Path] = []
        if re.match(r"^\d{4}-\d{2}$", source_dir.name):
            comp_dirs = [source_dir]
        else:
            all_comps = sorted(
                [
                    d
                    for d in source_dir.iterdir()
                    if d.is_dir() and re.match(r"^\d{4}-\d{2}$", d.name)
                ]
            )
            if all_comps:
                if kwargs.get("all_partitions") or kwargs.get("all"):
                    comp_dirs = all_comps
                elif comp_filter := kwargs.get("competencia"):
                    req = [c.strip() for c in comp_filter.split(",") if c.strip()]
                    comp_dirs = [d for d in all_comps if d.name in req]
                else:
                    comp_dirs = [all_comps[-1]]
            else:
                comp_dirs = [source_dir]

        total_rows = 0
        total_files = 0
        t0_all = time.time()

        for comp_dir in comp_dirs:
            comp = (
                comp_dir.name
                if re.match(r"^\d{4}-\d{2}$", comp_dir.name)
                else (kwargs.get("competencia") or "2026-10")
            )
            logger.info("============================================================")
            logger.info("Iniciando Competência RFB: %s", comp)
            logger.info("============================================================")

            table_arg = (
                kwargs.get("table") or kwargs.get("table_name") or kwargs.get("group")
            )
            if table_arg:
                target_groups = [table_arg] if table_arg in TABLE_SCHEMAS else []
            else:
                target_groups = TABLE_ORDER

            loaded_files: set[str] = set()
            if not force:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT nome_arquivo
                        FROM rfb_cnpj.arquivo_carregado
                        WHERE dataset = 'rfb_cnpj' AND particao = %s;
                        """,
                        (comp,),
                    )
                    loaded_files = {r[0] for r in cur.fetchall()}

            for group in target_groups:
                zip_files: list[Path] = []
                g_dir = comp_dir / group
                if g_dir.is_dir():
                    zip_files.extend(sorted(g_dir.glob("*.zip")))
                else:
                    for f in sorted(comp_dir.glob("*.zip")):
                        if group in f.name.lower():
                            zip_files.append(f)

                if not zip_files:
                    continue

                part_name = self.ensure_partition(
                    conn, group, comp, tablespace=tablespace
                )
                logger.info(
                    "  -> Grupo '%s': %d arquivos para partição '%s.%s'",
                    group,
                    len(zip_files),
                    self.schema,
                    part_name,
                )

                for zf in zip_files:
                    if not force and zf.name in loaded_files:
                        logger.info(
                            "    PULANDO [%s] %s (já carregado)", group, zf.name
                        )
                        continue

                    local_zip = zf
                    is_staged = False
                    try:
                        if zf.parent.resolve() != staging_dir.resolve():
                            local_zip = stage_file_locally(zf, staging_dir)
                            is_staged = True

                        t_file0 = time.time()
                        rows = self._load_single_item(
                            conn,
                            source_path=local_zip,
                            filename=zf.name,
                            table_name=group,
                            competencia=comp,
                            staging_dir=staging_dir,
                            force=force,
                            tablespace=tablespace,
                        )
                        elapsed_f = time.time() - t_file0
                        total_rows += rows
                        total_files += 1
                        loaded_files.add(zf.name)
                        logger.info(
                            "    [%s] %s: %s linhas em %.2fs",
                            group,
                            zf.name,
                            f"{rows:,}",
                            elapsed_f,
                        )
                    except Exception as exc:
                        logger.error("Erro ao carregar %s: %s", zf.name, exc)
                        if not conn.autocommit:
                            conn.rollback()
                        raise
                    finally:
                        if is_staged and local_zip.exists():
                            for _ in range(5):
                                try:
                                    local_zip.unlink(missing_ok=True)
                                    break
                                except PermissionError:
                                    time.sleep(0.5)

        elapsed_total = time.time() - t0_all
        logger.info("============================================================")
        logger.info(
            "Carga RFB Concluída: %d arquivos, %s linhas em %.1fs (%.2f min)",
            total_files,
            f"{total_rows:,}",
            elapsed_total,
            elapsed_total / 60,
        )
        logger.info("============================================================")
        return total_rows
