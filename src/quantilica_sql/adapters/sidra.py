"""Adaptador para tabelas agregadas do IBGE SIDRA."""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from quantilica.core.lake import select_current
from quantilica.core.manifests import MANIFEST_SIDECAR_SUFFIX

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

_RE_TABELA_NO_NOME = re.compile(r"^t-(\d+)(?:_|@|\.|$)")
_RE_CARIMBO = re.compile(r"@(\d{8})(?:T\d{6})?\.json$")
_NAO_SAO_DADOS = ("metadados@", "indice_pesquisas@")
_SEM_DATA = dt.date(1, 1, 1)  # ``@00010101``: o SIDRA não informou a modificação


def _parse_modificacao(value: Any, *, origem: str) -> dt.date | None:
    """Converte uma data de modificação em ``date``; ausente ou ``0001-01-01`` vira
    ``None``. Valor ilegível levanta ``ValueError`` (nunca vira um padrão)."""
    if value is None or value == "":
        return None
    if isinstance(value, dt.datetime):
        parsed: dt.date | None = value.date()
    elif isinstance(value, dt.date):
        parsed = value
    else:
        texto = str(value).strip()
        parsed = None
        try:
            if re.fullmatch(r"\d{8}", texto):
                parsed = dt.datetime.strptime(texto, "%Y%m%d").date()
            elif re.fullmatch(r"\d{2}/\d{2}/\d{4}", texto):
                parsed = dt.datetime.strptime(texto, "%d/%m/%Y").date()
            else:
                parsed = dt.date.fromisoformat(texto[:10])
        except ValueError:
            parsed = None
        if parsed is None:
            raise ValueError(
                f"data de modificacao inválida em {origem}: {value!r} "
                "(esperado AAAA-MM-DD ou AAAAMMDD)"
            )
    return None if parsed == _SEM_DATA else parsed


def modificacao_do_arquivo(path: Path) -> dt.date | None:
    """Data de modificação (do período, na fonte) de um arquivo bruto do SIDRA.

    Ordem: ``metadata.modificacao`` do sidecar ``.manifest.json`` (escrita pelo
    fetcher a partir da API) e, na falta dele, o carimbo ``@AAAAMMDD`` do nome (a
    mesma data). ``0001-01-01``/``@00010101`` (a API não informou) e a ausência de
    ambos dão ``None``. A data de coleta (``fetched_at``, ``metadados@...``) não
    entra: não é a data da fonte.
    """
    sidecar = path.with_name(path.name + MANIFEST_SIDECAR_SUFFIX)
    if sidecar.is_file():
        try:
            manifesto = json.loads(sidecar.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            manifesto = None
        meta = manifesto.get("metadata") if isinstance(manifesto, dict) else None
        if isinstance(meta, dict):
            data = _parse_modificacao(meta.get("modificacao"), origem=sidecar.name)
            if data is not None:
                return data
    m = _RE_CARIMBO.search(path.name)
    return _parse_modificacao(m.group(1), origem=path.name) if m else None


def _id_da_tabela(path: Path) -> str | None:
    """Id da tabela pelo layout do bruto (``t-<id>_...`` ou pasta ``t-<id>``)."""
    for nome in (path.name, path.parent.name):
        m = _RE_TABELA_NO_NOME.match(nome)
        if m:
            return m.group(1)
    return None


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
                modificacao DATE,
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
            # Tabelas criadas antes: ``modificacao`` era NOT NULL DEFAULT CURRENT_DATE
            # (data da carga, que não é a data da fonte). Sem data na fonte, é NULL.
            cur.execute(
                """
                SELECT is_nullable, column_default FROM information_schema.columns
                WHERE table_schema = %s AND table_name = 'dados'
                  AND column_name = 'modificacao';
                """,
                (self.schema,),
            )
            col = cur.fetchone()
            if col and (col[0] == "NO" or col[1] is not None):
                cur.execute(
                    sql.SQL(
                        "ALTER TABLE {schema}.dados "
                        "ALTER COLUMN modificacao DROP NOT NULL, "
                        "ALTER COLUMN modificacao DROP DEFAULT;"
                    ).format(schema=sql.Identifier(self.schema))
                )
        commit_if_standalone(conn)

    def _arquivos_vigentes(self, raiz: Path, tabela: str | None) -> list[Path]:
        """Arquivos de dados da pasta, só a versão vigente (``@AAAAMMDD`` mais
        novo) de cada consulta, em ordem. ``metadados@``, ``indice_pesquisas@``,
        sidecars e ``*.tmp`` não são dados."""
        rels = [
            p.relative_to(raiz).as_posix()
            for p in raiz.rglob("*.json")
            if p.is_file()
            and not p.name.startswith(_NAO_SAO_DADOS)
            and not p.name.endswith(MANIFEST_SIDECAR_SUFFIX)
        ]
        vigentes = select_current(rels)
        if len(vigentes) < len(rels):
            logger.info(
                "SIDRA: %d arquivo(s) de versões antigas ficam de fora (vigente por "
                "consulta).",
                len(rels) - len(vigentes),
            )
        arquivos = [raiz / rel for rel in vigentes]
        if tabela is not None:
            alvo = tabela.removeprefix("t-")
            arquivos = [a for a in arquivos if _id_da_tabela(a) == alvo]
        return arquivos

    def _load_directory(
        self, conn: psycopg.Connection, raiz: Path, **kwargs: Any
    ) -> int:
        tabela = kwargs.get("table")
        tabela = str(tabela) if tabela else None
        if not (kwargs.get("all_partitions") or kwargs.get("all")) and tabela is None:
            raise ValueError(
                f"{raiz} é uma pasta: passe --all para carregar todas as tabelas "
                "(a versão vigente de cada arquivo) ou --table <id> para uma tabela."
            )
        arquivos = self._arquivos_vigentes(raiz, tabela)
        if not arquivos:
            alvo = f" da tabela {tabela}" if tabela else ""
            raise ValueError(
                f"nenhum arquivo de dados{alvo} do SIDRA em {raiz} "
                "(esperado t-<id>/t-<id>_...@AAAAMMDD.json)."
            )
        total = 0
        for arquivo in arquivos:
            total += self.load(conn, arquivo, **{**kwargs, "table": None})
        return total

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        if source_path is not None and Path(source_path).is_dir():
            return self._load_directory(conn, Path(source_path), **kwargs)

        table_id = kwargs.get("table_id")
        if table_id is None:
            if source_path is not None:
                table_id = _id_da_tabela(source_path)
                if table_id is None:
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

        # Data da fonte: o campo do próprio item (se vier), senão a do arquivo
        # (sidecar ou carimbo do nome). Sem nenhuma, NULL: nunca um valor fixo.
        if "modificacao" in kwargs:
            modificacao_arquivo = _parse_modificacao(
                kwargs["modificacao"], origem="o argumento modificacao"
            )
        elif source_path is not None and Path(source_path).is_file():
            modificacao_arquivo = modificacao_do_arquivo(Path(source_path))
        else:
            modificacao_arquivo = None

        def modificacao_do(item: dict[str, Any]) -> dt.date | None:
            if "modificacao" in item:
                return _parse_modificacao(
                    item["modificacao"], origem=f"um item da tabela {table_id_str}"
                )
            return modificacao_arquivo

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
                            modificacao_arquivo,
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
                            modificacao_do(item),
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
                            modificacao_do(item),
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
