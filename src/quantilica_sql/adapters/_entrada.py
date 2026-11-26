"""Resolvedor de entrada compartilhado pelos adaptadores que leem uma pasta do bruto.

O bruto guarda versões carimbadas (``nome@AAAAMMDD[THHMMSS].ext``). Um adaptador
que recebe uma pasta (a raiz do bruto ou a pasta de uma tabela/série) com ``--all``
ou ``--table`` precisa da lista dos arquivos de dados **vigentes**: a versão mais
nova de cada arquivo (``quantilica.core.lake.select_current``), sem sidecars
``.manifest.json``, sem ``*.tmp`` e sem os arquivos auxiliares da fonte
(metadados, índices, catálogos).

Cada adaptador diz o que é "arquivo de dados" e qual é a chave da tabela/série de
um arquivo; o resto (varredura, versão vigente, filtro por chave e erros) é daqui.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

from quantilica.core.lake import select_current
from quantilica.core.manifests import MANIFEST_SIDECAR_SUFFIX

__all__ = ["arquivos_vigentes"]

_TEMP_SUFFIX = ".tmp"


def arquivos_vigentes(
    raiz: Path,
    *,
    all_partitions: bool,
    table: str | None,
    eh_dado: Callable[[Path], bool],
    chave_de: Callable[[Path], str | None],
    fonte: str,
    esperado: str,
    nome_chave: str = "tabela",
    flag_chave: str = "--table <id>",
    normaliza_chave: Callable[[str], str] = str,
    glob: str = "*.json",
    logger: logging.Logger | None = None,
) -> list[Path]:
    """Arquivos de dados vigentes de uma pasta do bruto, em ordem.

    Args:
        raiz: pasta a varrer (recursivamente).
        all_partitions: ``--all``; carrega todas as tabelas/séries da pasta.
        table: ``--table``; só a tabela/série com essa chave. Vale também sem
            ``all_partitions``.
        eh_dado: ``True`` para o arquivo de dados da fonte (recebe o caminho
            completo); ``False`` para os auxiliares (metadados, índices...).
            Sidecars ``.manifest.json`` e ``*.tmp`` nunca chegam a ele.
        chave_de: chave da tabela/série de um arquivo de dados (``None`` quando o
            arquivo não diz a qual pertence). Comparada com ``table`` depois de
            ``normaliza_chave`` nos dois lados.
        fonte: nome da fonte, para as mensagens (``"SIDRA"``).
        esperado: layout esperado, para a mensagem de pasta sem dados.
        nome_chave: ``"tabela"`` ou ``"série"``, para as mensagens.
        flag_chave: como o usuário pede uma só (``"--table <id>"``).
        normaliza_chave: normalização da chave (p. ex. tirar o prefixo ``t-``).
        glob: padrão dos arquivos candidatos.
        logger: onde avisar quantas versões antigas ficaram de fora.

    Returns:
        Caminhos absolutos (``raiz / relativo``), um por arquivo vigente.

    Raises:
        ValueError: pasta sem ``all_partitions`` nem ``table``; ou nenhum arquivo
            de dados (com ``table``: nenhum com essa chave).
    """
    chave = None if table is None or table == "" else normaliza_chave(str(table))
    if not all_partitions and chave is None:
        raise ValueError(
            f"{raiz} é uma pasta: passe --all para carregar todas as {nome_chave}s "
            f"(a versão vigente de cada arquivo) ou {flag_chave} para uma "
            f"{nome_chave}."
        )
    rels = [
        p.relative_to(raiz).as_posix()
        for p in raiz.rglob(glob)
        if p.is_file()
        and not p.name.endswith(MANIFEST_SIDECAR_SUFFIX)
        and not p.name.endswith(_TEMP_SUFFIX)
        and eh_dado(p)
    ]
    vigentes = select_current(rels)
    if logger is not None and len(vigentes) < len(rels):
        logger.info(
            "%s: %d arquivo(s) de versões antigas ficam de fora (vigente por "
            "consulta).",
            fonte,
            len(rels) - len(vigentes),
        )
    arquivos = [raiz / rel for rel in vigentes]
    if chave is not None:
        arquivos = [
            a
            for a in arquivos
            if (c := chave_de(a)) is not None and normaliza_chave(c) == chave
        ]
    if not arquivos:
        alvo = f" da {nome_chave} {table}" if chave is not None else ""
        raise ValueError(
            f"nenhum arquivo de dados{alvo} do {fonte} em {raiz} (esperado {esperado})."
        )
    return arquivos
