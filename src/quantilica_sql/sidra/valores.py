"""Valor de uma célula do SIDRA: número exato ou símbolo da fonte.

O campo ``V`` da API é texto. Número vira :class:`~decimal.Decimal` com a escala do
texto (``"7695.8200000000000"`` volta idêntico). Os símbolos da legenda do IBGE
(``-`` zero não resultante de arredondamento, ``..`` não se aplica, ``...`` não
disponível, ``X`` omitido para não identificar o informante, ``Z`` zero resultante
de arredondamento) e o vazio ficam em ``simbolo``, com ``valor`` nulo: nunca viram
``0``, e nunca há valor nulo sem o símbolo. Zero numérico (``0``, ``0.0``) é número.
"""

from __future__ import annotations

import re
from decimal import Decimal

from quantilica_sql.sidra.erros import SidraFonteError

SIMBOLOS = frozenset({"-", "..", "...", "X", "Z", ""})
_RE_NUMERO = re.compile(r"-?\d+(?:\.\d+)?")


def converter_valor(
    texto: str, *, origem: str = "valor"
) -> tuple[Decimal | None, str | None]:
    """``(valor, simbolo)``: exatamente um dos dois é ``None``.

    Raises:
        SidraFonteError: texto que não é número nem símbolo conhecido (a carga
            para em vez de adivinhar).
    """
    if texto in SIMBOLOS:
        return None, texto
    if _RE_NUMERO.fullmatch(texto):
        return Decimal(texto), None
    limpo = texto.strip()
    if limpo != texto:
        return converter_valor(limpo, origem=origem)
    raise SidraFonteError(
        f"{origem}: valor {texto!r} não é número nem símbolo conhecido do SIDRA "
        f"({', '.join(repr(s) for s in sorted(SIMBOLOS - {''}))} ou vazio)"
    )
