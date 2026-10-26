"""Adaptador genérico para cotações históricas da B3 (COTAHIST) no PostgreSQL."""

from __future__ import annotations

import io
import logging
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.core.txscope import commit_if_standalone
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.b3")


class B3Adapter(BaseSourceAdapter):
    """Adaptador de carga para séries históricas de cotações COTAHIST da B3."""

    @property
    def domain(self) -> str:
        return "b3"

    @property
    def schema(self) -> str:
        return "b3"

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
                        "FROM {}.arquivo_carregado WHERE dataset = 'b3';"
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

    def _ensure_cotacoes_table(
        self, conn: psycopg.Connection, tablespace: str | None = None
    ) -> None:
        ensure_schema(conn, self.schema)
        tb_clause = (
            sql.SQL(" TABLESPACE {}").format(sql.Identifier(tablespace))
            if tablespace and tablespace != "pg_default"
            else sql.SQL("")
        )
        ddl = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.cotacoes_historicas (
                data_pregao DATE NOT NULL,
                cod_bdi VARCHAR(4),
                cod_negociacao VARCHAR(12) NOT NULL,
                tipo_mercado SMALLINT,
                nome_resumido VARCHAR(20),
                especificacao_papel VARCHAR(10),
                prazo_dias_termo VARCHAR(3),
                moeda_referencia VARCHAR(4),
                preco_abertura NUMERIC(13,2),
                preco_maximo NUMERIC(13,2),
                preco_minimo NUMERIC(13,2),
                preco_medio NUMERIC(13,2),
                preco_fechamento NUMERIC(13,2),
                preco_melhor_oferta_compra NUMERIC(13,2),
                preco_melhor_oferta_venda NUMERIC(13,2),
                total_negocios INTEGER,
                quantidade_titulos_negociados BIGINT,
                volume_total_titulos_negociados NUMERIC(17,2),
                preco_exercicio NUMERIC(13,2),
                indicador_correcao_preco SMALLINT,
                data_vencimento DATE,
                fator_cotacao INTEGER,
                preco_exercicio_pontos NUMERIC(13,6),
                codigo_isin VARCHAR(12),
                num_distribuicao_papel VARCHAR(3),
                _arquivo TEXT
            ){}
            """
        ).format(sql.Identifier(self.schema), tb_clause)
        with conn.cursor() as cur:
            cur.execute(ddl)
        commit_if_standalone(conn)

    def parse_cotahist_line(self, line: str, filename: str) -> list[str] | None:
        """Faz o parsing dos campos posicionais de uma linha do COTAHIST (tipo 01)."""
        if len(line) < 245 or not line.startswith("01"):
            return None
        dt_str = line[2:10]
        data_pregao = f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]}"
        cod_bdi = line[10:12].strip()
        cod_neg = line[12:24].strip()
        tp_merc = line[24:27].strip()
        nom_res = line[27:39].strip()
        especi = line[39:49].strip()
        prazot = line[49:52].strip()
        modref = line[52:56].strip()

        def parse_num(s: str, dec: int = 2) -> str:
            val = s.strip()
            if not val or not val.isdigit():
                return ""
            v = int(val) / (10**dec)
            return f"{v:.{dec}f}"

        p_aber = parse_num(line[56:69])
        p_max = parse_num(line[69:82])
        p_min = parse_num(line[82:95])
        p_med = parse_num(line[95:108])
        p_fech = parse_num(line[108:121])
        p_ofc = parse_num(line[121:134])
        p_ofv = parse_num(line[134:147])
        tot_neg = line[147:152].strip()
        qtd_tit = line[152:170].strip()
        vol_tot = parse_num(line[170:188])
        p_exerc = parse_num(line[188:201])
        ind_corr = line[201:202].strip()

        dt_venc_raw = line[202:210].strip()
        dt_venc = (
            f"{dt_venc_raw[:4]}-{dt_venc_raw[4:6]}-{dt_venc_raw[6:8]}"
            if dt_venc_raw.isdigit() and int(dt_venc_raw[:4]) > 1900
            else ""
        )
        fat_cot = line[210:217].strip()
        p_exerc_pts = parse_num(line[217:230], dec=6)
        isin = line[230:242].strip()
        num_dist = line[242:245].strip()

        return [
            data_pregao,
            cod_bdi,
            cod_neg,
            tp_merc,
            nom_res,
            especi,
            prazot,
            modref,
            p_aber,
            p_max,
            p_min,
            p_med,
            p_fech,
            p_ofc,
            p_ofv,
            tot_neg,
            qtd_tit,
            vol_tot,
            p_exerc,
            ind_corr,
            dt_venc,
            fat_cot,
            p_exerc_pts,
            isin,
            num_dist,
            filename,
        ]

    def load_cotahist_file(
        self,
        conn: psycopg.Connection,
        file_path: Path,
        tablespace: str | None = None,
        sha256: str | None = None,
        source_name: str | None = None,
    ) -> int:
        filename = source_name or file_path.name
        self._ensure_cotacoes_table(conn, tablespace=tablespace)
        copy_buf = io.StringIO()
        count = 0

        with open(file_path, encoding="latin-1", errors="replace") as f:
            for line in f:
                parsed = self.parse_cotahist_line(line, filename)
                if parsed:
                    copy_buf.write(";".join(parsed) + "\n")
                    count += 1

        with self.atomic_file(
            conn,
            filename=filename,
            dataset="b3",
            particao=Path(filename).stem,
            sha256=sha256,
            table="cotacoes_historicas",
        ):
            if count > 0:
                copy_buf.seek(0)
                copy_sql = sql.SQL(
                    """
                    COPY {}.cotacoes_historicas
                    FROM STDIN WITH (FORMAT csv, DELIMITER ';', NULL '')
                    """
                ).format(sql.Identifier(self.schema))
                with conn.cursor() as cur:
                    with cur.copy(copy_sql) as copy:
                        while chunk := copy_buf.read(1024 * 1024):
                            copy.write(chunk)

            self.record_and_commit(
                conn,
                filename=filename,
                dataset="b3",
                rows_loaded=count,
                particao=Path(filename).stem,
                metadata={"tabela": "cotacoes_historicas"},
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
            logger.warning("Caminho B3 não encontrado: %s", source_dir)
            return 0

        force = kwargs.get("force", False)
        tablespace = kwargs.get("tablespace")
        staging_dir = kwargs.get("staging_dir")

        all_files: list[Path] = []
        if source_dir.is_file():
            all_files = [source_dir]
        else:
            for item in source_dir.rglob("*"):
                if item.is_file() and (
                    "COTAHIST" in item.name.upper()
                    or item.suffix.lower() in (".txt", ".csv")
                ):
                    all_files.append(item)

        all_files.sort(key=lambda x: x.name)
        total_rows = 0
        total_loaded = 0
        t0_all = time.time()

        logger.info(
            "Iniciando carga de %d arquivos B3 a partir de %s",
            len(all_files),
            source_dir,
        )

        for fpath in all_files:
            do_load, digest = self.should_load_file(
                conn, fpath, dataset="b3", particao=fpath.stem, force=force
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

                rows = self.load_cotahist_file(
                    conn,
                    local_file,
                    tablespace=tablespace,
                    sha256=digest,
                    source_name=fpath.name,
                )
                total_rows += rows
                total_loaded += 1
                logger.info(
                    "  [b3] %s: %s cotações em %.2fs",
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
            "Carga B3 concluída: %d arquivos, %s cotações em %.2fs",
            total_loaded,
            f"{total_rows:,}",
            time.time() - t0_all,
        )
        return total_rows
