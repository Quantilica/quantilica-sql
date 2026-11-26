"""Leitura do SIDRA: metadados, papel das dimensões e células tipadas (sem banco)."""

from quantilica_sql.sidra.erros import SidraFonteError
from quantilica_sql.sidra.leitura import (
    ArquivoSidra,
    Celula,
    LocalidadeCelula,
    Serie,
    ler_arquivo,
)
from quantilica_sql.sidra.metadados import (
    Metadados,
    Periodo,
    ler_metadados,
    metadados_da_pasta,
    metadados_vigente,
)
from quantilica_sql.sidra.papeis import (
    CLASSIFICACAO,
    PERIODO,
    TERRITORIO,
    VARIAVEL,
    Papel,
    papeis_por_cabecalho,
    papeis_por_url,
    resolver_papeis,
)
from quantilica_sql.sidra.valores import SIMBOLOS, converter_valor

__all__ = [
    "CLASSIFICACAO",
    "PERIODO",
    "SIMBOLOS",
    "TERRITORIO",
    "VARIAVEL",
    "ArquivoSidra",
    "Celula",
    "LocalidadeCelula",
    "Metadados",
    "Papel",
    "Periodo",
    "Serie",
    "SidraFonteError",
    "converter_valor",
    "ler_arquivo",
    "ler_metadados",
    "metadados_da_pasta",
    "metadados_vigente",
    "papeis_por_cabecalho",
    "papeis_por_url",
    "resolver_papeis",
]
