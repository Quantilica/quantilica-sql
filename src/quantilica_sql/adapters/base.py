"""Interface base e métodos utilitários genéricos para adaptadores de carga."""

from __future__ import annotations

import logging
import re
import time
import zipfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from quantilica_sql.config import get_settings
from quantilica_sql.core.atomic import atomic_file_load, file_sha256
from quantilica_sql.core.tracker import (
    backfill_sha256,
    ensure_tracker_table,
    file_status,
    is_file_loaded,
    record_loaded_file,
)
from quantilica_sql.core.txscope import commit_if_standalone
from quantilica_sql.database import ensure_schema

logger = logging.getLogger("quantilica_sql.adapters.base")


class BaseSourceAdapter(ABC):
    """Contrato abstrato que cada fonte deve implementar para cargas no PostgreSQL."""

    @property
    @abstractmethod
    def domain(self) -> str:
        """Identificador canônico do domínio (ex: pdet, rfb, cvm)."""
        ...

    @property
    @abstractmethod
    def schema(self) -> str:
        """Nome do schema PostgreSQL de destino."""
        ...

    def resolve_source_dir(self, source_path: Path | None = None) -> Path:
        """Resolve o diretório da fonte a partir do parâmetro ou de storage_root."""
        if source_path is not None:
            return Path(source_path)
        settings = get_settings()
        return settings.storage_root / self.domain

    def stage_file_locally(
        self,
        src: Path,
        staging_dir: Path | None = None,
        max_retries: int = 10,
    ) -> Path:
        """Copia arquivo para SSD/disco local com retomada incremental contra stalls."""
        if not staging_dir:
            settings = get_settings()
            staging_dir = settings.staging_dir / f"{self.domain}_staging"
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
                        f"Tamanho copiado ({actual}) difere do esperado "
                        f"({expected_size})"
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

    def normalize_identifier(self, name: str) -> str:
        """Normaliza nome de coluna ou tabela para padrão Postgres."""
        clean = name.strip().lower()
        clean = re.sub(r"[^\w]+", "_", clean)
        clean = clean.strip("_")
        return clean or "col"

    def ensure_table_with_schema_evolution(
        self,
        conn: psycopg.Connection,
        table_name: str,
        columns: list[str],
        tablespace: str | None = None,
        add_source_col: bool = True,
    ) -> list[str]:
        """Cria tabela se não existir e adiciona colunas faltantes dinamicamente."""
        ensure_schema(conn, self.schema)
        norm_cols = [self.normalize_identifier(c) for c in columns]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s;
                """,
                (self.schema, table_name),
            )
            existing = {r[0].lower() for r in cur.fetchall()}

            if not existing:
                cols_defs = [
                    sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in norm_cols
                ]
                if add_source_col:
                    cols_defs.append(sql.SQL("_arquivo TEXT"))
                tb_clause = (
                    sql.SQL(" TABLESPACE {}").format(sql.Identifier(tablespace))
                    if tablespace and tablespace != "pg_default"
                    else sql.SQL("")
                )
                ddl = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({}){}").format(
                    sql.Identifier(self.schema),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(cols_defs),
                    tb_clause,
                )
                cur.execute(ddl)
            else:
                for c in norm_cols:
                    if c not in existing:
                        cur.execute(
                            sql.SQL(
                                "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} TEXT"
                            ).format(
                                sql.Identifier(self.schema),
                                sql.Identifier(table_name),
                                sql.Identifier(c),
                            )
                        )
                if add_source_col and "_arquivo" not in existing:
                    cur.execute(
                        sql.SQL(
                            "ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS _arquivo TEXT"
                        ).format(
                            sql.Identifier(self.schema),
                            sql.Identifier(table_name),
                        )
                    )

        commit_if_standalone(conn)
        return norm_cols

    def is_file_already_loaded(
        self,
        conn: psycopg.Connection,
        filename: str,
        dataset: str | None = None,
        particao: str = "",
    ) -> bool:
        """Verifica se o arquivo já foi carregado no ledger de cargas."""
        ensure_tracker_table(conn, self.schema)
        ds = dataset or self.domain
        return is_file_loaded(
            conn,
            schema=self.schema,
            filename=filename,
            dataset=ds,
            particao=particao,
        )

    def should_load_file(
        self,
        conn: psycopg.Connection,
        path: Path,
        *,
        dataset: str | None = None,
        particao: str = "",
        force: bool = False,
        filename: str | None = None,
    ) -> tuple[bool, str]:
        """Decide se o arquivo deve ser carregado, comparando o ``sha256`` ao ledger.

        Carrega quando é novo, foi republicado com outro hash ou ``force``. Registro
        legado (sem hash) é preservado: só ganha o hash, sem recarga.

        Returns:
            ``(carregar, sha256)``; passe o hash a ``atomic_file``.
        """
        digest = file_sha256(path)
        if force:
            return True, digest
        name = filename or path.name
        ds = dataset or self.domain
        status = file_status(conn, self.schema, name, ds, particao, digest)
        if status == "legacy":
            backfill_sha256(conn, self.schema, name, ds, particao, digest)
            return False, digest
        if status == "changed":
            logger.info("%s/%s (%s) mudou na origem; recarregando.", ds, particao, name)
        return status in ("new", "changed"), digest

    def atomic_file(
        self,
        conn: psycopg.Connection,
        *,
        filename: str,
        dataset: str | None = None,
        particao: str = "",
        sha256: str | None = None,
        table: str | None = None,
    ):
        """Escopo transacional da carga de um arquivo (dados + ledger juntos).

        Use ao redor do COPY e do ``record_and_commit``; substitui a carga anterior
        do mesmo arquivo. Ver ``quantilica_sql.core.atomic``.
        """
        return atomic_file_load(
            conn,
            schema=self.schema,
            table=table,
            filename=filename,
            dataset=dataset or self.domain,
            particao=particao,
            sha256=sha256,
        )

    def record_and_commit(
        self,
        conn: psycopg.Connection,
        filename: str,
        dataset: str | None = None,
        rows_loaded: int = 0,
        particao: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Registra o arquivo carregado no ledger e comita a transação."""
        ensure_tracker_table(conn, self.schema)
        ds = dataset or self.domain
        record_loaded_file(
            conn,
            schema=self.schema,
            filename=filename,
            dataset=ds,
            rows_loaded=rows_loaded,
            particao=particao,
            metadata=metadata or {},
        )
        commit_if_standalone(conn)

    @abstractmethod
    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        """Executa a rotina de carga e retorna o total de linhas inseridas."""
        ...

    @abstractmethod
    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        """Retorna sumário estruturado de tabelas, contagens e última carga."""
        ...
