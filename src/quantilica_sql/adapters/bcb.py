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

from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.core.copy_stream import copy_from_rows
from quantilica_sql.core.tracker import (
    ensure_tracker_table,
    is_file_loaded,
    record_loaded_file,
)
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.bcb")


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
            series_data_rows = 0
            if series_data_exists:
                cur.execute(
                    sql.SQL("SELECT count(*) FROM {schema}.series_data;").format(
                        schema=sql.Identifier(self.schema)
                    )
                )
                series_data_rows = cur.fetchone()[0]

            return {
                "domain": self.domain,
                "schema": self.schema,
                "series_carregadas": series_count,
                "linhas_totais": total_rows,
                "series_data_rows": series_data_rows,
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
        if not conn.autocommit:
            conn.commit()

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        series_id = kwargs.get("series_id")
        if series_id is None:
            if source_path is not None:
                m = re.search(r"\d+", source_path.stem)
                series_id = int(m.group(0)) if m else 0
            else:
                series_id = int(kwargs.get("particao", 0))

        series_id_str = str(series_id)
        filename = kwargs.get("filename") or (
            source_path.name if source_path else f"series_{series_id_str}.json"
        )

        if is_file_loaded(
            conn,
            schema=self.schema,
            filename=filename,
            dataset=self.domain,
            particao=series_id_str,
        ):
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
        if raw_items is None and source_path is not None and source_path.exists():
            with open(source_path, encoding="utf-8") as f:
                parsed_json = json.load(f)
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
                raw_date = item.get("date") or item.get("data")
                dt_val = _parse_bcb_date(raw_date)
                if not dt_val:
                    continue
                raw_val = item.get("value") or item.get("valor")
                val = _parse_bcb_value(raw_val)
                raw_end = item.get("date_end") or item.get("data_fim")
                dt_end = _parse_bcb_date(raw_end) if raw_end else None
                s_id = int(item.get("series_id", series_id))
                ativo = bool(item.get("ativo", True))
                formatted_rows.append((s_id, dt_val, dt_end, val, ativo))

        cols = ["series_id", "date", "date_end", "value", "ativo"]
        if formatted_rows:
            rows_loaded = copy_from_rows(
                conn,
                schema=self.schema,
                table="series_data",
                columns=cols,
                rows=formatted_rows,
            )
        else:
            rows_loaded = 0

        record_loaded_file(
            conn,
            schema=self.schema,
            filename=filename,
            dataset=self.domain,
            rows_loaded=rows_loaded,
            particao=series_id_str,
            metadata={"series_id": int(series_id)},
        )
        return rows_loaded
