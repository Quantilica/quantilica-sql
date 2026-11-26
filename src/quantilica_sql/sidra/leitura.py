"""Leitura de um arquivo de dados do SIDRA em células tipadas.

``ler_arquivo`` lê o JSON da API (``/values``) sem importar nenhum fetcher, resolve o
papel de cada dimensão (:mod:`quantilica_sql.sidra.papeis`) e devolve cada linha
como uma :class:`Celula`: a série (variável, unidade e categorias, na ordem das
classificações dos metadados), a localidade, o período e o valor (número ou
símbolo). Funções puras, sem banco.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, NamedTuple

import orjson

from quantilica_sql.sidra.erros import SidraFonteError
from quantilica_sql.sidra.metadados import Metadados, metadados_da_pasta
from quantilica_sql.sidra.papeis import (
    CLASSIFICACAO,
    PERIODO,
    TERRITORIO,
    VARIAVEL,
    Papel,
    resolver_papeis,
)
from quantilica_sql.sidra.valores import converter_valor

MARCA_CABECALHO = "Nível Territorial (Código)"


class Serie(NamedTuple):
    """Variável, unidade de medida e categorias de uma célula.

    ``categorias`` traz ``(classificacao_id, categoria_id, nome)`` na ordem das
    classificações dos metadados (não a da URL).
    """

    variavel_id: int
    variavel_nome: str
    unidade_codigo: str
    unidade_nome: str
    categorias: tuple[tuple[int, int, str], ...]


class LocalidadeCelula(NamedTuple):
    nivel_codigo: str
    nivel_nome: str
    codigo: str
    nome: str


class Celula(NamedTuple):
    serie: Serie
    localidade: LocalidadeCelula
    periodo: str
    valor: Decimal | None
    simbolo: str | None


@dataclass(frozen=True)
class ArquivoSidra:
    """Um arquivo de dados já lido."""

    caminho: Path
    tabela_id: int
    metadados: Metadados
    papeis: list[Papel]
    celulas: list[Celula] = field(repr=False)


def _inteiro(texto: str, *, onde: str) -> int:
    try:
        return int(texto)
    except ValueError as exc:
        raise SidraFonteError(f"{onde}: {texto!r} não é um código numérico") from exc


def ler_arquivo(caminho: Path, metadados: Metadados | None = None) -> ArquivoSidra:
    """Lê ``caminho`` e devolve as células tipadas e o papel de cada dimensão.

    Args:
        caminho: arquivo de dados do SIDRA (resposta crua do ``/values``).
        metadados: metadados da tabela; sem eles, o ``metadados@...`` mais novo da
            pasta do arquivo.

    Raises:
        SidraFonteError: arquivo ilegível, sem metadados, papéis que não se
            resolvem, valor que não é número nem símbolo, código não numérico.
    """
    origem = str(caminho)
    try:
        linhas: Any = orjson.loads(caminho.read_bytes())
    except (OSError, orjson.JSONDecodeError) as exc:
        raise SidraFonteError(f"{origem}: JSON ilegível ({exc})") from exc
    if not isinstance(linhas, list) or not linhas or not isinstance(linhas[0], dict):
        raise SidraFonteError(f"{origem}: esperada uma lista de objetos (API /values)")
    if linhas[0].get("NC") != MARCA_CABECALHO:
        raise SidraFonteError(
            f"{origem}: sem a linha de cabeçalho do SIDRA (consulte com /h/y); o "
            "nome das dimensões é o que identifica o papel de cada uma"
        )
    if metadados is None:
        metadados = metadados_da_pasta(caminho.parent)
    papeis = resolver_papeis(caminho, linhas, metadados)
    if metadados is None:  # resolver_papeis já recusou; só para o tipo
        raise SidraFonteError(f"{origem}: sem metadados")

    dims = {}
    for i, papel in enumerate(papeis, 1):
        dims.setdefault(papel.papel, []).append((i, papel))
    t = dims[TERRITORIO][0][0]
    v = dims[VARIAVEL][0][0]
    p = dims[PERIODO][0][0]
    # classificações na ordem dos metadados, não da URL
    cats = sorted(
        (
            (metadados.classificacao(pp.classificacao).ordem, i, pp.classificacao)  # type: ignore[union-attr]
            for i, pp in dims.get(CLASSIFICACAO, [])
        ),
    )
    cols_cat = [(cid, f"D{i}C", f"D{i}N") for _ordem, i, cid in cats]
    kt_c, kt_n, kv_c, kv_n, kp_c = f"D{t}C", f"D{t}N", f"D{v}C", f"D{v}N", f"D{p}C"

    cache_serie: dict[tuple[str, ...], Serie] = {}
    cache_loc: dict[tuple[str, str, str, str], LocalidadeCelula] = {}
    celulas: list[Celula] = []
    for n, r in enumerate(linhas[1:], 2):
        try:
            chave = (
                r[kv_c],
                r[kv_n],
                r["MC"],
                r["MN"],
                *(x for _c, kc, kn in cols_cat for x in (r[kc], r[kn])),
            )
            serie = cache_serie.get(chave)
            if serie is None:
                onde = f"{origem} (linha {n})"
                serie = cache_serie[chave] = Serie(
                    _inteiro(r[kv_c], onde=onde),
                    r[kv_n],
                    r["MC"],
                    r["MN"],
                    tuple(
                        (cid, _inteiro(r[kc], onde=onde), r[kn])
                        for cid, kc, kn in cols_cat
                    ),
                )
            lk = (r["NC"], r["NN"], r[kt_c], r[kt_n])
            loc = cache_loc.get(lk)
            if loc is None:
                loc = cache_loc[lk] = LocalidadeCelula(*lk)
            valor, simbolo = converter_valor(r["V"], origem=f"{origem} (linha {n})")
            celulas.append(Celula(serie, loc, r[kp_c], valor, simbolo))
        except KeyError as exc:
            raise SidraFonteError(
                f"{origem} (linha {n}): falta o campo {exc.args[0]!r}"
            ) from exc
    return ArquivoSidra(
        caminho=caminho,
        tabela_id=metadados.tabela_id,
        metadados=metadados,
        papeis=papeis,
        celulas=celulas,
    )
