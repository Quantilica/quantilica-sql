"""Adaptador para tabelas agregadas do IBGE SIDRA."""

from __future__ import annotations

import json
import logging
import re
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
from quantilica_sql.core.txscope import commit_if_standalone
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.sidra")


class SidraAdapter(BaseSourceAdapter):
    """Adaptador de carga para dados do IBGE SIDRA."""

    @property
    def domain(self) -> str:
        return "sidra"

    @property
    def schema(self) -> str:
        return "ibge_sidra"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        ensure_schema(conn, self.schema)
        ensure_tracker_table(conn, self.schema)
        with conn.cursor() as cur:
            query = sql.SQL(
                """
                SELECT count(*), coalesce(sum(linhas), 0) 
                FROM {schema}.{table} 
                WHERE dataset IN ('sidra', 'tabela_sidra');
                """
            ).format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier("arquivo_carregado"),
            )
            cur.execute(query)
            res = cur.fetchone()
            tables_count, total_rows = res if res else (0, 0)

            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = %s AND table_name = 'dados'
                );
                """,
                (self.schema,),
            )
            dados_exists = cur.fetchone()[0]
            dados_rows = 0
            if dados_exists:
                cur.execute(
                    sql.SQL("SELECT count(*) FROM {schema}.dados;").format(
                        schema=sql.Identifier(self.schema)
                    )
                )
                dados_rows = cur.fetchone()[0]

            return {
                "domain": self.domain,
                "schema": self.schema,
                "tabelas_carregadas": tables_count,
                "linhas_totais": total_rows,
                "dados_rows": dados_rows,
            }

    def _ensure_tables(self, conn: psycopg.Connection) -> None:
        """Garante a existência da tabela e índices de dados do SIDRA."""
        ensure_schema(conn, self.schema)
        create_sql = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {schema}.dados (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                tabela_sidra_id TEXT NOT NULL,
                localidade_id BIGINT NOT NULL,
                dimensao_id BIGINT NOT NULL,
                periodo_id INTEGER NOT NULL,
                modificacao DATE NOT NULL DEFAULT CURRENT_DATE,
                ativo BOOLEAN NOT NULL DEFAULT TRUE,
                v TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_dados_ativo 
            ON {schema}.dados (tabela_sidra_id, localidade_id, dimensao_id, periodo_id) 
            WHERE ativo;
            """
        ).format(schema=sql.Identifier(self.schema))
        with conn.cursor() as cur:
            cur.execute(create_sql)
        commit_if_standalone(conn)

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        table_id = kwargs.get("table_id")
        if table_id is None:
            if source_path is not None:
                m = re.search(r"\d+", source_path.stem)
                table_id = m.group(0) if m else source_path.stem
            else:
                table_id = kwargs.get("particao", "0")

        table_id_str = str(table_id)
        filename = kwargs.get("filename") or (
            source_path.name if source_path else f"table_{table_id_str}.json"
        )

        if is_file_loaded(
            conn,
            schema=self.schema,
            filename=filename,
            dataset=self.domain,
            particao=table_id_str,
        ):
            logger.info(
                "Tabela SIDRA %s (%s) já carregada em %s.arquivo_carregado. Pulando.",
                table_id_str,
                filename,
                self.schema,
            )
            return 0

        self._ensure_tables(conn)

        raw_items = kwargs.get("dados_rows") or kwargs.get("rows") or kwargs.get("data")
        if raw_items is None and source_path is not None and source_path.exists():
            with open(source_path, encoding="utf-8") as f:
                parsed_json = json.load(f)
            if isinstance(parsed_json, dict):
                raw_items = (
                    parsed_json.get("dados_rows")
                    or parsed_json.get("dados")
                    or parsed_json.get("rows")
                    or parsed_json.get("data")
                    or [parsed_json]
                )
            elif isinstance(parsed_json, list):
                raw_items = parsed_json

        raw_items = raw_items or []
        if (
            raw_items
            and isinstance(raw_items[0], dict)
            and raw_items[0].get("NC") == "Nível Territorial (Código)"
        ):
            raw_items = raw_items[1:]

        formatted_rows: list[tuple[Any, ...]] = []
        for item in raw_items:
            if isinstance(item, (list, tuple)):
                if len(item) == 7:
                    formatted_rows.append(tuple(item))
                elif len(item) == 5:
                    formatted_rows.append(
                        (
                            item[0],
                            item[1],
                            item[2],
                            item[3],
                            "2026-01-01",
                            True,
                            str(item[4]),
                        )
                    )
            elif isinstance(item, dict):
                if "tabela_sidra_id" in item:
                    formatted_rows.append(
                        (
                            str(item.get("tabela_sidra_id", table_id_str)),
                            int(item.get("localidade_id", 0)),
                            int(item.get("dimensao_id", 0)),
                            int(item.get("periodo_id", 0)),
                            item.get("modificacao", "2026-01-01"),
                            bool(item.get("ativo", True)),
                            str(item.get("v", "")),
                        )
                    )
                else:
                    v_val = str(item.get("V", item.get("v", "")))
                    loc_id = (
                        int(item.get("D1C", 0))
                        if str(item.get("D1C", "")).isdigit()
                        else 0
                    )
                    dim_id = (
                        int(item.get("D2C", 0))
                        if str(item.get("D2C", "")).isdigit()
                        else 0
                    )
                    per_id = (
                        int(item.get("D3C", 0))
                        if str(item.get("D3C", "")).isdigit()
                        else 0
                    )
                    formatted_rows.append(
                        (
                            table_id_str,
                            loc_id,
                            dim_id,
                            per_id,
                            item.get("modificacao", "2026-01-01"),
                            True,
                            v_val,
                        )
                    )

        cols = [
            "tabela_sidra_id",
            "localidade_id",
            "dimensao_id",
            "periodo_id",
            "modificacao",
            "ativo",
            "v",
        ]
        with self.atomic_file(
            conn,
            filename=filename,
            dataset=self.domain,
            particao=table_id_str,
        ):
            if formatted_rows:
                rows_loaded = copy_from_rows(
                    conn,
                    schema=self.schema,
                    table="dados",
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
                particao=table_id_str,
                metadata={"tabela_sidra_id": table_id_str},
            )
        return rows_loaded
