"""Metadados vigentes de uma tabela do SIDRA (``metadados@AAAAMMDD.json``).

O arquivo é o que o fetcher grava a partir da API de agregados do IBGE:
variáveis, classificações com categorias, períodos já interpretados (frequência,
ano, mês, trimestre...) e localidades. A versão vigente é a mais nova da pasta
(``quantilica.core.lake.select_current``).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, NamedTuple

import orjson
from quantilica.core.lake import select_current

from quantilica_sql.sidra.erros import SidraFonteError

PREFIXO_METADADOS = "metadados@"


class Variavel(NamedTuple):
    id: int
    nome: str
    unidade: str


class Categoria(NamedTuple):
    id: int
    nome: str
    unidade: str | None
    nivel: int


@dataclass(frozen=True)
class Classificacao:
    id: int
    nome: str
    ordem: int  # posição entre as classificações da tabela (a dos metadados)
    categorias: tuple[Categoria, ...]


@dataclass(frozen=True)
class Periodo:
    codigo: str
    rotulo: str
    frequencia: str | None
    ano: int | None
    mes: int | None
    trimestre: int | None
    semestre: int | None
    data_inicio: dt.date | None
    data_fim: dt.date | None
    modificacao: dt.date | None


class Localidade(NamedTuple):
    nivel_codigo: str  # "3" (a API de metadados diz "N3"; a de dados, NC = "3")
    nivel_nome: str
    codigo: str
    nome: str


@dataclass(frozen=True)
class Metadados:
    """Metadados de uma tabela, já tipados. ``bruto`` é o JSON original."""

    tabela_id: int
    nome: str
    url: str | None
    pesquisa: str | None
    assunto: str | None
    periodicidade: str | None
    variaveis: tuple[Variavel, ...]
    classificacoes: tuple[Classificacao, ...]
    periodos: dict[str, Periodo]
    localidades: tuple[Localidade, ...]
    bruto: dict[str, Any] = field(repr=False, compare=False)
    arquivo: Path | None = None

    def classificacao(self, id_: int) -> Classificacao | None:
        for c in self.classificacoes:
            if c.id == id_:
                return c
        return None


def _data(valor: Any) -> dt.date | None:
    if not valor:
        return None
    return dt.date.fromisoformat(str(valor)[:10])


def _int(valor: Any) -> int | None:
    return None if valor is None else int(valor)


def metadados_vigente(pasta: Path) -> Path | None:
    """O ``metadados@AAAAMMDD.json`` mais novo da pasta, ou ``None``."""
    nomes = [
        p.name
        for p in pasta.glob(f"{PREFIXO_METADADOS}*.json")
        if p.is_file() and not p.name.endswith(".manifest.json")
    ]
    if not nomes:
        return None
    return pasta / select_current(nomes)[-1]


def parse_metadados(bruto: dict[str, Any], *, origem: str = "metadados") -> Metadados:
    """Converte o JSON dos metadados em :class:`Metadados`."""
    try:
        classificacoes = tuple(
            Classificacao(
                id=int(c["id"]),
                nome=c["nome"],
                ordem=i,
                categorias=tuple(
                    Categoria(
                        int(x["id"]),
                        x["nome"],
                        x.get("unidade"),
                        int(x.get("nivel", 0)),
                    )
                    for x in c["categorias"]
                ),
            )
            for i, c in enumerate(bruto["classificacoes"])
        )
        periodos = {
            str(p["id"]): Periodo(
                codigo=str(p["id"]),
                rotulo=(p.get("literals") or [str(p["id"])])[0],
                frequencia=p.get("frequencia"),
                ano=_int(p.get("ano")),
                mes=_int(p.get("mes")),
                trimestre=_int(p.get("trimestre")),
                semestre=_int(p.get("semestre")),
                data_inicio=_data(p.get("data_inicio")),
                data_fim=_data(p.get("data_fim")),
                modificacao=_data(p.get("modificacao")),
            )
            for p in bruto["periodos"]
        }
        localidades = tuple(
            Localidade(
                str(loc["nivel"]["id"]).removeprefix("N"),
                loc["nivel"]["nome"],
                str(loc["id"]),
                loc["nome"],
            )
            for loc in bruto["localidades"]
        )
        periodicidade = bruto.get("periodicidade") or {}
        return Metadados(
            tabela_id=int(bruto["id"]),
            nome=bruto["nome"],
            url=bruto.get("url"),
            pesquisa=(bruto.get("pesquisa") or {}).get("nome"),
            assunto=bruto.get("assunto"),
            periodicidade=periodicidade.get("frequencia"),
            variaveis=tuple(
                Variavel(int(v["id"]), v["nome"], v.get("unidade") or "")
                for v in bruto["variaveis"]
            ),
            classificacoes=classificacoes,
            periodos=periodos,
            localidades=localidades,
            bruto=bruto,
        )
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise SidraFonteError(
            f"{origem}: metadados fora do formato esperado ({type(exc).__name__}: "
            f"{exc})"
        ) from exc


def ler_metadados(arquivo: Path) -> Metadados:
    """Lê e converte um ``metadados@AAAAMMDD.json``."""
    try:
        bruto = orjson.loads(arquivo.read_bytes())
    except (OSError, orjson.JSONDecodeError) as exc:
        raise SidraFonteError(f"{arquivo}: metadados ilegíveis ({exc})") from exc
    if not isinstance(bruto, dict):
        raise SidraFonteError(f"{arquivo}: metadados não são um objeto JSON")
    meta = parse_metadados(bruto, origem=str(arquivo))
    return replace(meta, arquivo=arquivo)


def metadados_da_pasta(pasta: Path) -> Metadados | None:
    """Metadados vigentes da pasta da tabela, ou ``None`` se não há nenhum."""
    arquivo = metadados_vigente(pasta)
    return None if arquivo is None else ler_metadados(arquivo)
