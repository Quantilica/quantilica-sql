"""Interface base para adaptadores de fonte do quantilica-sql."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import psycopg


class BaseSourceAdapter(ABC):
    """Contrato abstrato que cada fonte deve implementar para cargas no PostgreSQL."""

    @property
    @abstractmethod
    def domain(self) -> str:
        """Identificador canônico do domínio (ex: pdet, rfb_cnpj, sidra)."""
        ...

    @property
    @abstractmethod
    def schema(self) -> str:
        """Nome do schema PostgreSQL de destino."""
        ...

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
