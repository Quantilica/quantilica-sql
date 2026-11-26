"""Erro de leitura de um arquivo do SIDRA."""

from __future__ import annotations


class SidraFonteError(ValueError):
    """O arquivo (ou os metadados) do SIDRA não permite uma leitura segura.

    A mensagem sempre diz qual arquivo e o motivo. A leitura nunca assume uma
    ordem fixa das dimensões: quando a fonte não basta, o erro explica o que falta.
    """
