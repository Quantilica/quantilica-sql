"""Papel de cada dimensão ``D<i>`` de um arquivo de dados do SIDRA.

O SIDRA numera ``D1..Dn`` na ordem dos segmentos da URL da consulta, não pela
tabela: a mesma tabela dá ``D1`` território num arquivo e ``D1`` período em outro. A
posição, portanto, nunca é a regra. Há duas fontes independentes do papel:

* a **URL** da consulta (campo ``url`` do sidecar ``.manifest.json``): a ordem dos
  segmentos ``n<k>``, ``v``, ``p`` e ``c<id>`` é a ordem de ``D1..Dn`` e já traz o id
  de cada classificação (:func:`papeis_por_url`);
* o **cabeçalho** (primeira linha: o nome de cada ``D<i>N``) com os **metadados**:
  ``Variável`` é a variável, a dimensão cujo nome é o de uma classificação dos
  metadados é essa classificação, a cujos códigos estão todos nos períodos dos
  metadados é o período e a que sobra é o território, conferido por ``NC``/``NN``
  (:func:`papeis_por_cabecalho`).

:func:`resolver_papeis` usa a URL, confere com o cabeçalho e, se divergirem, se uma
dimensão ficar sem papel ou se faltarem os metadados, levanta
:class:`SidraFonteError` com o nome do arquivo e o motivo.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urlsplit

from quantilica.core.manifests import read_manifest_sidecar

from quantilica_sql.sidra.erros import SidraFonteError
from quantilica_sql.sidra.metadados import Metadados, metadados_da_pasta

TERRITORIO = "territorio"
VARIAVEL = "variavel"
PERIODO = "periodo"
CLASSIFICACAO = "classificacao"

NOME_VARIAVEL = "Variável"
_RE_TERRITORIO = re.compile(r"n\d+")
_RE_CLASSIFICACAO = re.compile(r"c(\d+)")
_IGNORADOS = frozenset({"t", "h", "f", "d"})  # tabela, cabeçalho, formato, decimais
_RE_DIM_CODIGO = re.compile(r"D(\d+)C")


class Papel(NamedTuple):
    """Papel de uma dimensão; ``classificacao`` só para ``CLASSIFICACAO``."""

    papel: str
    classificacao: int | None = None

    def __str__(self) -> str:
        return (
            f"{self.papel}({self.classificacao})"
            if self.classificacao is not None
            else self.papel
        )


def _descreve(papeis: Sequence[Papel]) -> str:
    return ", ".join(f"D{i}={p}" for i, p in enumerate(papeis, 1))


def papeis_por_url(url: str, *, origem: str = "url") -> list[Papel]:
    """Papéis de ``D1..Dn`` pela ordem dos segmentos da URL do ``/values``.

    Vários ``n<k>`` (``/n1/all/n3/all``) formam **um** território, na posição do
    primeiro. ``t``, ``h``, ``f`` e ``d`` não são dimensões.
    """
    segmentos = [s for s in urlsplit(url).path.split("/") if s]
    if "values" in segmentos:
        segmentos = segmentos[segmentos.index("values") + 1 :]
    if len(segmentos) % 2:
        raise SidraFonteError(f"{origem}: URL com segmento sem valor: {url}")
    papeis: list[Papel] = []
    for chave, _valor in zip(segmentos[::2], segmentos[1::2], strict=True):
        if chave in _IGNORADOS:
            continue
        if _RE_TERRITORIO.fullmatch(chave):
            if Papel(TERRITORIO) not in papeis:
                papeis.append(Papel(TERRITORIO))
        elif chave == "v":
            papeis.append(Papel(VARIAVEL))
        elif chave == "p":
            papeis.append(Papel(PERIODO))
        elif m := _RE_CLASSIFICACAO.fullmatch(chave):
            papeis.append(Papel(CLASSIFICACAO, int(m.group(1))))
        else:
            raise SidraFonteError(f"{origem}: segmento {chave!r} desconhecido na URL")
    for papel in (TERRITORIO, VARIAVEL, PERIODO):
        n = sum(1 for p in papeis if p.papel == papel)
        if n != 1:
            raise SidraFonteError(
                f"{origem}: a URL tem {n} dimensões de {papel} (esperada 1): {url}"
            )
    ids = [p.classificacao for p in papeis if p.papel == CLASSIFICACAO]
    if len(ids) != len(set(ids)):
        raise SidraFonteError(f"{origem}: classificação repetida na URL: {url}")
    return papeis


def _dimensoes(cabecalho: Mapping[str, str]) -> list[int]:
    return sorted(
        int(m.group(1)) for k in cabecalho if (m := _RE_DIM_CODIGO.fullmatch(k))
    )


def papeis_por_cabecalho(
    linhas: Sequence[Mapping[str, str]],
    metadados: Metadados,
    *,
    origem: str = "arquivo",
) -> list[Papel]:
    """Papéis de ``D1..Dn`` pelo cabeçalho (``linhas[0]``) e pelos metadados.

    Independente da URL. Uma dimensão que se encaixa em mais de um papel, dois
    papéis iguais ou uma que não se encaixa em nenhum (e sobra mais de um
    território) levantam :class:`SidraFonteError`.
    """
    if not linhas:
        raise SidraFonteError(f"{origem}: arquivo vazio (sem cabeçalho)")
    cabecalho, dados = linhas[0], linhas[1:]
    dims = _dimensoes(cabecalho)
    if not dims:
        raise SidraFonteError(f"{origem}: cabeçalho sem colunas D<i>C")
    if not dados:
        raise SidraFonteError(
            f"{origem}: sem linhas de dados; o papel não se deduz só do cabeçalho"
        )
    por_nome: dict[str, list[int]] = {}
    for c in metadados.classificacoes:
        por_nome.setdefault(c.nome.strip().casefold(), []).append(c.id)
    periodos = set(metadados.periodos)

    papeis: list[Papel | None] = []
    sem_papel: list[int] = []
    for i in dims:
        nome = (cabecalho.get(f"D{i}N") or "").strip()
        candidatos: list[Papel] = []
        if nome.casefold() == NOME_VARIAVEL.casefold():
            candidatos.append(Papel(VARIAVEL))
        ids = por_nome.get(nome.casefold(), [])
        if len(ids) > 1:
            raise SidraFonteError(
                f"{origem}: papel ambíguo em D{i} ({nome!r}): duas classificações "
                f"dos metadados têm esse nome ({ids})"
            )
        candidatos.extend(Papel(CLASSIFICACAO, c) for c in ids)
        if candidatos:
            # com o nome batendo, só o nome decide (os códigos de uma classificação
            # podem coincidir com os de um período)
            if len(candidatos) > 1:
                raise SidraFonteError(
                    f"{origem}: papel ambíguo em D{i} ({nome!r}): serve de variável "
                    "e de classificação"
                )
            papeis.append(candidatos[0])
            continue
        codigos = {r[f"D{i}C"] for r in dados}
        if codigos <= periodos:
            papeis.append(Papel(PERIODO))
        else:
            papeis.append(None)
            sem_papel.append(i)

    if len(sem_papel) > 1:
        raise SidraFonteError(
            f"{origem}: dimensões sem papel identificável: "
            f"{', '.join(f'D{i} ({cabecalho.get(f"D{i}N")!r})' for i in sem_papel)} "
            "(nem variável, nem classificação dos metadados, nem período)"
        )
    if len(sem_papel) == 1:
        i = sem_papel[0]
        _confere_territorio(i, cabecalho, dados, metadados, origem)
        papeis[dims.index(i)] = Papel(TERRITORIO)
    final = [p for p in papeis if p is not None]
    for papel in (TERRITORIO, VARIAVEL, PERIODO):
        n = sum(1 for p in final if p.papel == papel)
        if n != 1:
            raise SidraFonteError(
                f"{origem}: papel ambíguo ou ausente: {n} dimensões de {papel} "
                f"(esperada 1); papéis: {_descreve(final)}"
            )
    ids = [p.classificacao for p in final if p.papel == CLASSIFICACAO]
    if len(ids) != len(set(ids)):
        raise SidraFonteError(f"{origem}: classificação repetida: {_descreve(final)}")
    return final


def _confere_territorio(
    i: int,
    cabecalho: Mapping[str, str],
    dados: Sequence[Mapping[str, str]],
    metadados: Metadados,
    origem: str,
) -> None:
    """O território deduzido por exclusão bate com ``NC``/``NN`` das linhas?

    Bate se o nome da dimensão contém todos os ``NN`` (``"Brasil e Unidade da
    Federação"`` contém ``"Brasil"`` e ``"Unidade da Federação"``) ou, na falta, se
    todo par (``NC``, ``D<i>C``) é uma localidade dos metadados.
    """
    nome = cabecalho.get(f"D{i}N") or ""
    nomes = {r["NN"] for r in dados}
    if all(n in nome for n in nomes):
        return
    conhecidas = {(loc.nivel_codigo, loc.codigo) for loc in metadados.localidades}
    if all((r["NC"], r[f"D{i}C"]) in conhecidas for r in dados):
        return
    raise SidraFonteError(
        f"{origem}: D{i} ({nome!r}) sobrou como território, mas não confere com "
        f"NC/NN das linhas (níveis {sorted(nomes)}) nem com as localidades dos "
        "metadados"
    )


def resolver_papeis(
    arquivo: Path,
    linhas: Sequence[Mapping[str, str]],
    metadados: Metadados | None = None,
) -> list[Papel]:
    """Papéis das dimensões de ``arquivo``: URL do sidecar conferida com o cabeçalho.

    Args:
        arquivo: o arquivo de dados (o sidecar é ``<arquivo>.manifest.json``).
        linhas: o JSON do arquivo (cabeçalho e linhas de dados).
        metadados: os metadados vigentes; sem eles, lê o ``metadados@...`` mais novo
            da pasta do arquivo.

    Raises:
        SidraFonteError: sem metadados; divergência entre a URL e o cabeçalho;
            dimensão sem papel; sem sidecar e sem como deduzir pelo cabeçalho.
    """
    origem = str(arquivo)
    if metadados is None:
        metadados = metadados_da_pasta(arquivo.parent)
    manifesto = read_manifest_sidecar(arquivo)
    url = manifesto.url if manifesto is not None and manifesto.url else None
    if metadados is None:
        causa = (
            "sem metadados@AAAAMMDD.json na pasta"
            if url
            else "sem sidecar .manifest.json e sem metadados@AAAAMMDD.json na pasta"
        )
        raise SidraFonteError(
            f"{origem}: {causa}; o carregador precisa dos metadados (classificações, "
            "períodos) e não assume a ordem das dimensões. Colete a tabela de novo "
            "com o fetcher ou copie o metadados@... junto com os dados."
        )
    cabecalho = linhas[0] if linhas else {}
    n_dims = len(_dimensoes(cabecalho))
    if len(linhas) <= 1:
        # sem linhas de dados: só a URL sabe o papel (nada a conferir)
        if url is None:
            raise SidraFonteError(
                f"{origem}: sem linhas de dados e sem sidecar; não há como saber o "
                "papel das dimensões"
            )
        por_url = papeis_por_url(url, origem=origem)
        _confere_contagem(por_url, n_dims, origem)
        _confere_classificacoes(por_url, metadados, origem)
        return por_url
    por_cabecalho = papeis_por_cabecalho(linhas, metadados, origem=origem)
    if url is None:
        return por_cabecalho
    por_url = papeis_por_url(url, origem=origem)
    if por_url != por_cabecalho:
        raise SidraFonteError(
            f"{origem}: a URL do sidecar e o cabeçalho divergem: URL "
            f"[{_descreve(por_url)}] x cabeçalho [{_descreve(por_cabecalho)}]"
        )
    _confere_classificacoes(por_url, metadados, origem)
    return por_url


def _confere_contagem(papeis: Sequence[Papel], n_dims: int, origem: str) -> None:
    if len(papeis) != n_dims:
        raise SidraFonteError(
            f"{origem}: a URL tem {len(papeis)} dimensões e o cabeçalho {n_dims}"
        )


def _confere_classificacoes(
    papeis: Sequence[Papel], metadados: Metadados, origem: str
) -> None:
    for p in papeis:
        if (
            p.papel == CLASSIFICACAO
            and metadados.classificacao(p.classificacao) is None
        ):
            raise SidraFonteError(
                f"{origem}: a classificação {p.classificacao} da URL não existe nos "
                "metadados da tabela"
            )
