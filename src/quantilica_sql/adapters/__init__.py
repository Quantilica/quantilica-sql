"""Registro e fábrica de adaptadores de fontes do quantilica-sql."""

from __future__ import annotations

from typing import TYPE_CHECKING

from quantilica_sql.adapters.bcb import BcbAdapter
from quantilica_sql.adapters.cvm import AnpAdapter, CvmAdapter, InmetAdapter
from quantilica_sql.adapters.pdet import PdetAdapter
from quantilica_sql.adapters.rfb import RfbAdapter
from quantilica_sql.adapters.sidra import SidraAdapter

if TYPE_CHECKING:
    from quantilica_sql.adapters.base import BaseSourceAdapter

_ADAPTERS: dict[str, type[BaseSourceAdapter]] = {
    "sidra": SidraAdapter,
    "bcb": BcbAdapter,
    "bcb-sgs": BcbAdapter,
    "pdet": PdetAdapter,
    "rfb": RfbAdapter,
    "rfb-cnpj": RfbAdapter,
    "cvm": CvmAdapter,
    "inmet": InmetAdapter,
    "anp": AnpAdapter,
}


def get_adapter(domain: str) -> BaseSourceAdapter:
    """Retorna uma instância do adaptador correspondente ao domínio informado."""
    clean_domain = domain.lower().strip()
    adapter_cls = _ADAPTERS.get(clean_domain)
    if not adapter_cls:
        raise ValueError(
            f"Adaptador não encontrado para o domínio '{domain}'. "
            f"Domínios suportados: {', '.join(sorted(set(_ADAPTERS.keys())))}"
        )
    return adapter_cls()


def list_supported_domains() -> list[str]:
    """Lista todos os domínios suportados."""
    return sorted(set(_ADAPTERS.keys()))
