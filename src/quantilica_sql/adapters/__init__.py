"""Registro e fábrica de adaptadores de fontes do quantilica-sql."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from quantilica_sql.adapters.anac import AnacAdapter
from quantilica_sql.adapters.anp import AnpAdapter
from quantilica_sql.adapters.b3 import B3Adapter
from quantilica_sql.adapters.bcb import BcbAdapter
from quantilica_sql.adapters.comex import ComexAdapter
from quantilica_sql.adapters.cvm import CvmAdapter
from quantilica_sql.adapters.datasus import DatasusAdapter
from quantilica_sql.adapters.inep import InepAdapter
from quantilica_sql.adapters.inmet import InmetAdapter
from quantilica_sql.adapters.pdet import PdetAdapter
from quantilica_sql.adapters.rfb import RfbAdapter
from quantilica_sql.adapters.rtn import RtnAdapter
from quantilica_sql.adapters.sidra import SidraAdapter
from quantilica_sql.adapters.tesouro_direto import TesouroDiretoAdapter
from quantilica_sql.adapters.tse import TseAdapter

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
    "tesouro-direto": TesouroDiretoAdapter,
    "tesouro_direto": TesouroDiretoAdapter,
    "tddata": TesouroDiretoAdapter,
    "rtn": RtnAdapter,
    "comex": ComexAdapter,
    "secex-comex": ComexAdapter,
    "anac": AnacAdapter,
    "b3": B3Adapter,
    "datasus": DatasusAdapter,
    "inep": InepAdapter,
    "tse": TseAdapter,
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
    if adapter_cls.deprecated:
        warnings.warn(
            f"Adaptador '{clean_domain}' obsoleto: {adapter_cls.deprecated}",
            FutureWarning,
            stacklevel=2,
        )
    return adapter_cls()


def list_supported_domains() -> list[str]:
    """Lista todos os domínios suportados."""
    return sorted(set(_ADAPTERS.keys()))
