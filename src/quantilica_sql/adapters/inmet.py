"""Adaptador genérico de carga para dados meteorológicos do INMET (BDMEP)."""

from __future__ import annotations

import csv
import io
import logging
import time
import zipfile
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.inmet")


class InmetAdapter(BaseSourceAdapter):
    """Adaptador de carga para estações e séries horárias do INMET BDMEP."""

    @property
    def domain(self) -> str:
        return "inmet"

    @property
    def schema(self) -> str:
        return "inmet"

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
                        "FROM {}.arquivo_carregado WHERE dataset = 'inmet';"
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

    def _ensure_estacoes_table(self, conn: psycopg.Connection) -> None:
        ensure_schema(conn, self.schema)
        ddl = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.estacoes (
                codigo_wmo VARCHAR(10) PRIMARY KEY,
                nome VARCHAR(100),
                regiao VARCHAR(10),
                uf VARCHAR(2),
                latitude NUMERIC(8,4),
                longitude NUMERIC(8,4),
                altitude NUMERIC(8,2),
                data_fundacao DATE
            );
            """
        ).format(sql.Identifier(self.schema))
        with conn.cursor() as cur:
            cur.execute(ddl)
        if not conn.autocommit:
            conn.commit()

    def _parse_station_header(
        self, stream: io.TextIOBase
    ) -> tuple[dict[str, Any] | None, list[str]]:
        reader = csv.reader(stream, delimiter=";")
        metadata: dict[str, Any] = {}
        try:
            r1 = next(reader, [])
            metadata["regiao"] = r1[1].strip() if len(r1) > 1 else ""
            r2 = next(reader, [])
            metadata["uf"] = r2[1].strip() if len(r2) > 1 else ""
            r3 = next(reader, [])
            metadata["nome"] = r3[1].strip() if len(r3) > 1 else ""
            r4 = next(reader, [])
            metadata["codigo_wmo"] = r4[1].strip() if len(r4) > 1 else ""
            r5 = next(reader, [])
            metadata["latitude"] = (
                r5[1].strip().replace(",", ".") if len(r5) > 1 else None
            )
            r6 = next(reader, [])
            metadata["longitude"] = (
                r6[1].strip().replace(",", ".") if len(r6) > 1 else None
            )
            r7 = next(reader, [])
            metadata["altitude"] = (
                r7[1].strip().replace(",", ".") if len(r7) > 1 else None
            )
            r8 = next(reader, [])
            metadata["data_fundacao"] = r8[1].strip() if len(r8) > 1 else None

            # 9ª linha: cabeçalho de medições
            header_row = next(reader, [])
            cols = [self.normalize_identifier(c) for c in header_row if c.strip()]
            return metadata, cols
        except Exception:
            return None, []

    def _load_station_csv_stream(
        self,
        conn: psycopg.Connection,
        stream_bytes: io.BufferedIOBase | Any,
        filename: str,
        year: str = "",
        tablespace: str | None = None,
    ) -> int:
        text_stream = io.StringIO(
            stream_bytes.read().decode("latin-1", errors="replace")
        )
        meta, cols = self._parse_station_header(text_stream)
        if not meta or not meta.get("codigo_wmo") or not cols:
            return 0

        # Atualiza tabela de estações
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {}.estacoes
                    (codigo_wmo, nome, regiao, uf, latitude, longitude, altitude)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (codigo_wmo) DO NOTHING;
                    """
                ).format(sql.Identifier(self.schema)),
                (
                    meta["codigo_wmo"],
                    meta.get("nome"),
                    meta.get("regiao"),
                    meta.get("uf"),
                    meta.get("latitude") or None,
                    meta.get("longitude") or None,
                    meta.get("altitude") or None,
                ),
            )

        table_name = f"medicoes_{year}" if year else "medicoes"
        norm_cols = ["codigo_wmo"] + [c for c in cols if c != "codigo_wmo"]
        self.ensure_table_with_schema_evolution(
            conn, table_name, norm_cols, tablespace=tablespace, add_source_col=True
        )

        # Monta buffer para COPY
        reader = csv.reader(text_stream, delimiter=";")
        wmo = meta["codigo_wmo"]
        copy_buf = io.StringIO()
        writer = csv.writer(copy_buf, delimiter=";", lineterminator="\n")
        count = 0
        for r in reader:
            if not r or len(r) < 2:
                continue
            row_data = [wmo] + [val.strip().replace(",", ".") for val in r[: len(cols)]]
            # Adiciona placeholder de _arquivo
            row_data.append(filename)
            writer.writerow(row_data)
            count += 1

        if count > 0:
            copy_buf.seek(0)
            copy_cols = norm_cols + ["_arquivo"]
            quoted_cols = sql.SQL(", ").join(sql.Identifier(c) for c in copy_cols)
            copy_sql = sql.SQL(
                """
                COPY {}.{} ({})
                FROM STDIN WITH (FORMAT csv, DELIMITER ';', NULL '')
                """
            ).format(
                sql.Identifier(self.schema),
                sql.Identifier(table_name),
                quoted_cols,
            )
            with conn.cursor() as cur:
                with cur.copy(copy_sql) as copy:
                    copy.write(copy_buf.getvalue())

        self.record_and_commit(
            conn,
            filename=filename,
            dataset="inmet",
            rows_loaded=count,
            particao=year,
            metadata={"estacao": wmo, "tabela": table_name},
        )
        return count

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        source_dir = self.resolve_source_dir(source_path)
        if not source_dir.exists():
            # Tenta fallback para inmet/bdmep
            alt = source_dir / "bdmep"
            if alt.exists():
                source_dir = alt
            else:
                logger.warning("Caminho INMET não encontrado: %s", source_dir)
                return 0

        self._ensure_estacoes_table(conn)
        force = kwargs.get("force", False)
        tablespace = kwargs.get("tablespace")
        staging_dir = kwargs.get("staging_dir")

        all_files: list[Path] = []
        if source_dir.is_file():
            all_files = [source_dir]
        else:
            for item in source_dir.rglob("*"):
                if item.is_file() and item.suffix.lower() in (".zip", ".csv"):
                    all_files.append(item)

        all_files.sort(key=lambda x: x.name)
        total_rows = 0
        total_loaded = 0
        t0_all = time.time()

        logger.info(
            "Iniciando carga de %d arquivos INMET a partir de %s",
            len(all_files),
            source_dir,
        )

        for fpath in all_files:
            if not force and self.is_file_already_loaded(
                conn, fpath.name, dataset="inmet"
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

                year_match = [
                    part for part in fpath.parts if part.isdigit() and len(part) == 4
                ]
                year = year_match[-1] if year_match else ""

                if local_file.suffix.lower() == ".zip":
                    with zipfile.ZipFile(local_file) as z:
                        csv_members = [
                            m for m in z.namelist() if m.lower().endswith(".csv")
                        ]
                        zip_rows = 0
                        for member in csv_members:
                            with z.open(member) as f:
                                rows = self._load_station_csv_stream(
                                    conn, f, member, year=year, tablespace=tablespace
                                )
                                zip_rows += rows
                        self.record_and_commit(
                            conn,
                            filename=fpath.name,
                            dataset="inmet",
                            rows_loaded=zip_rows,
                            particao=year,
                            metadata={"membros": len(csv_members)},
                        )
                        total_rows += zip_rows
                        total_loaded += 1
                else:
                    with open(local_file, "rb") as f:
                        rows = self._load_station_csv_stream(
                            conn, f, fpath.name, year=year, tablespace=tablespace
                        )
                    total_rows += rows
                    total_loaded += 1

                logger.info(
                    "  [inmet] %s carregado em %.2fs",
                    fpath.name,
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
            "Carga INMET concluída: %d arquivos, %s medições em %.2fs",
            total_loaded,
            f"{total_rows:,}",
            time.time() - t0_all,
        )
        return total_rows
