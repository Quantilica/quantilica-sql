"""Adaptador para séries temporais do BCB SGS."""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.adapters._entrada import arquivos_vigentes
from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.core.atomic import atomic_file_load
from quantilica_sql.core.copy_stream import copy_from_rows
from quantilica_sql.core.tracker import (
    ensure_tracker_table,
    is_file_loaded,
    record_loaded_file,
)
from quantilica_sql.core.txscope import commit_if_standalone
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.bcb")

# Arquivo de dados do bruto do bcb-sgs: ``series_<id>@AAAAMMDD[THHMMSS].json``.
# Os metadados (``bcb-sgs_AAAA-MM/metadata/<id>.json``) não casam.
_RE_ARQUIVO_DE_DADOS = re.compile(r"^series_(\d+)(?:@[0-9T]+)?\.json$")
_STAGING = "_qsql_bcb_stg"
_COLUNAS = ["series_id", "date", "date_end", "value", "ativo"]


def _id_da_serie(path: Path) -> str | None:
    """Id da série pelo nome do arquivo (``series_<id>@...json``)."""
    m = _RE_ARQUIVO_DE_DADOS.match(path.name)
    return m.group(1) if m else None


def _normaliza_serie(chave: str) -> str:
    """``433``, ``000433`` e ``series_433`` são a mesma série."""
    texto = chave.strip().removeprefix("series_")
    return str(int(texto)) if texto.isdigit() else texto


def _primeiro_nao_nulo(item: dict[str, Any], *chaves: str) -> Any:
    """Primeiro valor presente entre ``chaves`` (``0`` e ``""`` contam como
    presentes: só ``None`` e a chave ausente passam para a próxima)."""
    for chave in chaves:
        if item.get(chave) is not None:
            return item[chave]
    return None


def _parse_bcb_date(value: Any) -> dt.date | None:
    if value is None or value == "":
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    s = str(value).strip()
    if "/" in s:
        parts = s.split("/")
        if len(parts) == 3:
            try:
                return dt.date(int(parts[2]), int(parts[1]), int(parts[0]))
            except ValueError:
                return None
    try:
        return dt.date.fromisoformat(s[:10])
    except ValueError:
        return None


def _parse_bcb_value(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    s = str(value).strip().replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


class BcbAdapter(BaseSourceAdapter):
    """Adaptador de carga para séries temporais do BCB SGS."""

    @property
    def domain(self) -> str:
        return "bcb"

    @property
    def schema(self) -> str:
        return "bcb_sgs"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        ensure_tracker_table(conn, self.schema)
        with conn.cursor() as cur:
            query = sql.SQL(
                """
                SELECT count(*), coalesce(sum(linhas), 0) 
                FROM {schema}.{table} 
                WHERE dataset IN ('bcb', 'series');
                """
            ).format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier("arquivo_carregado"),
            )
            cur.execute(query)
            res = cur.fetchone()
            series_count, total_rows = res if res else (0, 0)

            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = 'series_data'
                );
                """,
                (self.schema,),
            )
            series_data_exists = cur.fetchone()[0]
            ativas = revisadas = distintas = 0
            if series_data_exists:
                tabela = sql.Identifier(self.schema, "series_data")
                cur.execute(
                    sql.SQL("SELECT count(*) FROM {} WHERE ativo;").format(tabela)
                )
                ativas = cur.fetchone()[0]
                cur.execute(
                    sql.SQL("SELECT count(*) FROM {} WHERE NOT ativo;").format(tabela)
                )
                revisadas = cur.fetchone()[0]
                cur.execute(
                    sql.SQL(
                        "SELECT count(*) FROM (SELECT DISTINCT series_id FROM {}) s;"
                    ).format(tabela)
                )
                distintas = cur.fetchone()[0]

            return {
                "domain": self.domain,
                "schema": self.schema,
                "series_carregadas": series_count,
                "linhas_totais": total_rows,
                "series_data_rows": ativas + revisadas,
                "series_data_ativas": ativas,
                "series_data_revisadas": revisadas,
                "series_distintas": distintas,
            }

    def _ensure_tables(self, conn: psycopg.Connection) -> None:
        """Garante a existência da tabela e índices de séries do BCB SGS."""
        ensure_schema(conn, self.schema)
        create_sql = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {schema}.series_data (
                series_id integer NOT NULL,
                date date NOT NULL,
                date_end date,
                value numeric,
                ativo boolean NOT NULL DEFAULT true,
                loaded_at timestamptz NOT NULL DEFAULT clock_timestamp()
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_bcb_sgs_series_date
            ON {schema}.series_data (series_id, date)
            WHERE ativo;
            """
        ).format(schema=sql.Identifier(self.schema))
        with conn.cursor() as cur:
            cur.execute(create_sql)
        commit_if_standalone(conn)

    def _load_directory(
        self, conn: psycopg.Connection, raiz: Path, **kwargs: Any
    ) -> int:
        serie = kwargs.get("table")
        arquivos = arquivos_vigentes(
            raiz,
            all_partitions=bool(kwargs.get("all_partitions") or kwargs.get("all")),
            table=str(serie) if serie else None,
            eh_dado=lambda p: _RE_ARQUIVO_DE_DADOS.match(p.name) is not None,
            chave_de=_id_da_serie,
            normaliza_chave=_normaliza_serie,
            fonte="BCB SGS",
            nome_chave="série",
            flag_chave="--table <id da série>",
            esperado="data/series_<id>@AAAAMMDDTHHMMSS.json",
            logger=logger,
        )
        total = 0
        for arquivo in arquivos:
            total += self.load(
                conn, arquivo, **{**kwargs, "table": None, "series_id": None}
            )
        return total

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        """Carrega um snapshot de série, ou uma pasta do bruto (``--all``/``--table``).

        Revisão: o snapshot é a série inteira. Onde o valor de um ponto difere do
        ativo, o ativo é desativado e o novo entra; ponto idêntico é no-op e ponto
        novo entra. Devolve quantas linhas novas entraram.
        """
        if source_path is not None and Path(source_path).is_dir():
            return self._load_directory(conn, Path(source_path), **kwargs)

        series_id = kwargs.get("series_id")
        if series_id is None:
            if source_path is not None:
                m = _RE_ARQUIVO_DE_DADOS.match(source_path.name) or re.search(
                    r"(\d+)", source_path.stem
                )
                series_id = int(m.group(1)) if m else 0
            else:
                series_id = int(kwargs.get("particao", 0))

        series_id_str = str(series_id)
        filename = kwargs.get("filename") or (
            source_path.name if source_path else f"series_{series_id_str}.json"
        )
        force = bool(kwargs.get("force"))

        digest: str | None = None
        if source_path is not None and Path(source_path).is_file():
            do_load, digest = self.should_load_file(
                conn,
                Path(source_path),
                particao=series_id_str,
                force=force,
                filename=filename,
            )
            ja_carregado = not do_load
        else:
            ja_carregado = not force and is_file_loaded(
                conn,
                schema=self.schema,
                filename=filename,
                dataset=self.domain,
                particao=series_id_str,
            )
        if ja_carregado:
            logger.info(
                "Série BCB %s (%s) já carregada em %s.arquivo_carregado. Pulando.",
                series_id_str,
                filename,
                self.schema,
            )
            return 0

        self._ensure_tables(conn)

        raw_items = (
            kwargs.get("data") or kwargs.get("rows") or kwargs.get("observations")
        )
        if raw_items is None and source_path is not None:
            if not Path(source_path).exists():
                raise FileNotFoundError(
                    f"arquivo da série BCB não existe: {source_path}"
                )
            with open(source_path, encoding="utf-8") as f:
                parsed_json = json.load(f, parse_float=Decimal)
            if isinstance(parsed_json, dict):
                raw_items = (
                    parsed_json.get("observations")
                    or parsed_json.get("data")
                    or parsed_json.get("rows")
                    or [parsed_json]
                )
            elif isinstance(parsed_json, list):
                raw_items = parsed_json

        raw_items = raw_items or []
        formatted_rows: list[tuple[Any, ...]] = []
        for item in raw_items:
            if isinstance(item, (list, tuple)):
                if len(item) == 5:
                    formatted_rows.append(tuple(item))
                elif len(item) == 3:
                    dt_val = _parse_bcb_date(item[1])
                    val = _parse_bcb_value(item[2])
                    if dt_val:
                        formatted_rows.append((int(item[0]), dt_val, None, val, True))
                elif len(item) == 2:
                    dt_val = _parse_bcb_date(item[0])
                    val = _parse_bcb_value(item[1])
                    if dt_val:
                        formatted_rows.append((int(series_id), dt_val, None, val, True))
            elif isinstance(item, dict):
                dt_val = _parse_bcb_date(_primeiro_nao_nulo(item, "date", "data"))
                if not dt_val:
                    continue
                # ``0`` é um valor (a Selic meta pode ser 0): só None é ausência.
                val = _parse_bcb_value(_primeiro_nao_nulo(item, "value", "valor"))
                raw_end = _primeiro_nao_nulo(item, "date_end", "data_fim")
                dt_end = _parse_bcb_date(raw_end) if raw_end else None
                s_id = int(item.get("series_id", series_id))
                ativo = bool(item.get("ativo", True))
                formatted_rows.append((s_id, dt_val, dt_end, val, ativo))

        formatted_rows = self._sem_repeticao(formatted_rows, filename)

        with atomic_file_load(
            conn,
            schema=self.schema,
            filename=filename,
            dataset=self.domain,
            particao=series_id_str,
            sha256=digest,
            replace=False,  # series_data não tem ``_arquivo``: a revisão é por ponto
        ):
            rows_loaded, revisadas = self._aplicar_revisao(conn, formatted_rows)
            record_loaded_file(
                conn,
                schema=self.schema,
                filename=filename,
                dataset=self.domain,
                rows_loaded=rows_loaded,
                particao=series_id_str,
                metadata={"series_id": int(series_id), "revisadas": revisadas},
            )
        if revisadas:
            logger.info(
                "Série BCB %s (%s): %d ponto(s) revisado(s), %d linha(s) nova(s).",
                series_id_str,
                filename,
                revisadas,
                rows_loaded,
            )
        return rows_loaded

    @staticmethod
    def _sem_repeticao(
        rows: list[tuple[Any, ...]], filename: str
    ) -> list[tuple[Any, ...]]:
        """Um ponto ativo por ``(series_id, date)`` no arquivo (o último vence): o
        índice único parcial não admite dois."""
        vistos: dict[tuple[Any, Any], int] = {}
        saida: list[tuple[Any, ...]] = []
        repetidos = 0
        for row in rows:
            if not row[4]:  # inativo (histórico explícito): não entra no índice
                saida.append(row)
                continue
            chave = (row[0], row[1])
            if chave in vistos:
                saida[vistos[chave]] = row
                repetidos += 1
            else:
                vistos[chave] = len(saida)
                saida.append(row)
        if repetidos:
            logger.warning(
                "%s: %d ponto(s) repetido(s) (mesma série e data); vale o último.",
                filename,
                repetidos,
            )
        return saida

    def _aplicar_revisao(
        self, conn: psycopg.Connection, rows: list[tuple[Any, ...]]
    ) -> tuple[int, int]:
        """COPY para a tabela temporária e, na mesma transação, desativa o ponto
        ativo cujo valor mudou e insere o que é novo ou revisado.

        Returns:
            ``(linhas novas inseridas, pontos revisados)``.
        """
        if not rows:
            return 0, 0
        destino = sql.Identifier(self.schema, "series_data")
        stg = sql.Identifier(_STAGING)
        stg_q = sql.Identifier("pg_temp", _STAGING)
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(stg_q))
            cur.execute(
                sql.SQL(
                    "CREATE TEMP TABLE {} (series_id integer NOT NULL, "
                    "date date NOT NULL, date_end date, value numeric, "
                    "ativo boolean NOT NULL)"
                ).format(stg)
            )
        copy_from_rows(
            conn,
            schema="pg_temp",
            table=_STAGING,
            columns=_COLUNAS,
            rows=rows,
        )
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    UPDATE {destino} t SET ativo = FALSE
                    FROM {stg} s
                    WHERE t.series_id = s.series_id AND t.date = s.date
                      AND t.ativo AND s.ativo
                      AND (t.value IS DISTINCT FROM s.value
                           OR t.date_end IS DISTINCT FROM s.date_end)
                    """
                ).format(destino=destino, stg=stg_q)
            )
            revisadas = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {destino} (series_id, date, date_end, value, ativo)
                    SELECT s.series_id, s.date, s.date_end, s.value, s.ativo
                    FROM {stg} s
                    WHERE NOT s.ativo OR NOT EXISTS (
                        SELECT 1 FROM {destino} t
                        WHERE t.series_id = s.series_id AND t.date = s.date
                          AND t.ativo
                    )
                    """
                ).format(destino=destino, stg=stg_q)
            )
            inseridas = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            cur.execute(sql.SQL("DROP TABLE {}").format(stg_q))
        return inseridas, revisadas
