"""Metadados das séries do BCB SGS: descoberta no bruto e leitura (sem banco).

O ``bcb-sgs-fetcher`` grava os metadados de cada série em
``bcb-sgs_AAAA-MM/metadata/<id com 6 dígitos>.json``, no formato
``{"basic": {...}, "full": {...}}`` (``dataclasses.asdict`` de
``SeriesMetadataBasic`` e ``SeriesMetadataFull``, datas em ISO). O mesmo ``<id>.json``
pode existir em vários meses; vale o mais novo. Este módulo só entende esse layout:
não importa o fetcher (o ``quantilica-sql`` não depende de nenhum).

O que a tabela ``series_metadata`` guarda são os campos de ``basic`` mais a data da
última atualização dos metadados (``full.last_update``). Descrição, metodologia e
formatos de divulgação ficam no bruto. Campo ausente vira nulo: nada é inventado.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

logger = logging.getLogger("quantilica_sql.adapters.bcb")

# ``000433.json`` (combinado) e ``000433_basic.json`` (só os dados básicos). O id tem
# 6 dígitos no fetcher; exigir 6 ou mais evita tomar ``433.json`` (dados) por metadado.
_RE_COMBINADO = re.compile(r"^(\d{6,})\.json$")
_RE_BASICO = re.compile(r"^(\d{6,})_basic\.json$")
_RE_MES = re.compile(r"^bcb-sgs_\d{4}-\d{2}$")

# Nome da pasta de metadados e o prefixo das pastas mensais do fetcher.
PASTA_METADADOS = "metadata"
PADRAO_PASTA_MES = "bcb-sgs_*"

# Dataset do ledger (``arquivo_carregado``); o dos dados é ``bcb``.
DATASET_METADADOS = "bcb-metadata"

# Colunas de ``series_metadata`` na ordem de ``MetadadoSerie.linha()``.
COLUNAS = [
    "series_id",
    "name",
    "name_abbreviated",
    "name_english",
    "name_english_abbreviated",
    "theme_hierarchy",
    "frequency",
    "unit",
    "source",
    "start_date",
    "end_date",
    "series_type",
    "precision",
    "min_value",
    "max_value",
    "manager_owner",
    "special",
    "formula",
    "primitive_series",
    "warning_message",
    "last_update",
]


@dataclass(frozen=True)
class MetadadoSerie:
    """Os campos de uma série como o SGS os publica; ausente é ``None``."""

    series_id: int
    name: str | None = None
    name_abbreviated: str | None = None
    name_english: str | None = None
    name_english_abbreviated: str | None = None
    theme_hierarchy: list[str] | None = None
    frequency: str | None = None
    unit: str | None = None
    source: str | None = None
    start_date: dt.date | None = None
    end_date: dt.date | None = None
    series_type: str | None = None
    precision: int | None = None
    min_value: Decimal | None = None
    max_value: Decimal | None = None
    manager_owner: str | None = None
    special: bool | None = None
    formula: str | None = None
    primitive_series: str | None = None
    warning_message: str | None = None
    last_update: dt.date | None = None

    def linha(self) -> tuple[Any, ...]:
        return tuple(getattr(self, c) for c in COLUNAS)


def eh_arquivo_de_metadados(path: Path) -> bool:
    """``True`` para ``<id>.json`` (6+ dígitos) ou ``<id>_basic.json``."""
    return id_do_metadado(path) is not None


def id_do_metadado(path: Path) -> int | None:
    """Id da série pelo nome do arquivo de metadados, ou ``None``."""
    m = _RE_COMBINADO.match(path.name) or _RE_BASICO.match(path.name)
    return int(m.group(1)) if m else None


def _pastas_de_metadados(raiz: Path) -> list[Path]:
    """Pastas de metadados em ordem de prioridade: ``metadata/`` direta primeiro (como
    o fetcher a procura), depois os meses do mais novo para o mais antigo.

    ``raiz`` pode ser a raiz do bruto, uma pasta mensal, a própria ``metadata/`` ou a
    ``data/`` (o irmão ``bcb-sgs_*/metadata`` é então procurado na pasta de cima).
    """
    bases = [raiz]
    if raiz.name == "data" and raiz.parent != raiz:
        bases.append(raiz.parent)
    pastas: list[Path] = []
    if raiz.name == PASTA_METADADOS and raiz.is_dir():
        pastas.append(raiz)
    for base in bases:
        direta = base / PASTA_METADADOS
        if direta.is_dir():
            pastas.append(direta)
    meses: list[Path] = []
    for base in bases:
        meses += [
            p / PASTA_METADADOS
            for p in base.glob(PADRAO_PASTA_MES)
            if _RE_MES.match(p.name) and (p / PASTA_METADADOS).is_dir()
        ]
    pastas += sorted(meses, key=lambda p: p.parent.name, reverse=True)
    return pastas


def arquivos_de_metadados(raiz: Path, *, serie: str | int | None = None) -> list[Path]:
    """Um arquivo de metadados por série (o da pasta de maior prioridade), em ordem
    de id. Na mesma pasta o combinado vence o ``_basic``. ``serie`` filtra uma série.

    Sidecars ``.manifest.json``, HTML bruto e ``*.tmp`` não casam com os nomes.
    """
    alvo = None
    if serie is not None and str(serie).strip() != "":
        texto = str(serie).strip().removeprefix("series_")
        alvo = int(texto) if texto.isdigit() else None
        if alvo is None:
            return []
    escolhidos: dict[int, Path] = {}
    for pasta in _pastas_de_metadados(raiz):
        locais: dict[int, Path] = {}
        for p in sorted(pasta.glob("*.json")):
            sid = id_do_metadado(p)
            if sid is None or (alvo is not None and sid != alvo) or not p.is_file():
                continue
            if sid not in locais or _RE_COMBINADO.match(p.name):
                locais[sid] = p
        for sid, p in locais.items():
            escolhidos.setdefault(sid, p)
    return [escolhidos[sid] for sid in sorted(escolhidos)]


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------
def _texto(valor: Any) -> str | None:
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto or None


def _inteiro(valor: Any) -> int | None:
    if valor is None or isinstance(valor, bool):
        return None
    try:
        return int(str(valor).strip())
    except ValueError:
        return None


def _decimal(valor: Any) -> Decimal | None:
    if valor is None or isinstance(valor, bool) or valor == "":
        return None
    try:
        return Decimal(str(valor).strip().replace(",", "."))
    except InvalidOperation:
        return None


def _data(valor: Any) -> dt.date | None:
    texto = _texto(valor)
    if texto is None:
        return None
    if "/" in texto:
        d, _, resto = texto.partition("/")
        m, _, a = resto.partition("/")
        try:
            return dt.date(int(a), int(m), int(d))
        except ValueError:
            return None
    try:
        return dt.date.fromisoformat(texto[:10])
    except ValueError:
        return None


def _booleano(valor: Any) -> bool | None:
    if isinstance(valor, bool):
        return valor
    texto = (_texto(valor) or "").lower()
    if texto in ("sim", "s", "true"):
        return True
    if texto in ("não", "nao", "n", "false"):
        return False
    return None


def _temas(valor: Any, origem: Path) -> list[str] | None:
    if valor is None:
        return None
    if not isinstance(valor, list):
        raise ValueError(
            f"{origem}: theme_hierarchy deveria ser lista, veio {type(valor).__name__}"
        )
    temas = [t for item in valor if (t := _texto(item)) is not None]
    return temas or None


def ler_metadado(path: Path) -> MetadadoSerie | None:
    """Lê um arquivo de metadados do fetcher.

    Returns:
        Os campos da série, ou ``None`` quando o arquivo é JSON válido mas não traz
        dados básicos (``{"basic": null}``): não há o que carregar.

    Raises:
        ValueError: JSON ilegível, formato inesperado, ou o id de dentro não é o do
            nome do arquivo. A mensagem traz o caminho.
    """
    sid_nome = id_do_metadado(path)
    if sid_nome is None:
        raise ValueError(f"{path}: nome não é de um arquivo de metadados do SGS")
    try:
        doc = json.loads(path.read_text(encoding="utf-8"), parse_float=Decimal)
    except (OSError, ValueError) as exc:
        raise ValueError(f"{path}: metadados do BCB SGS ilegíveis ({exc})") from exc
    if not isinstance(doc, dict):
        raise ValueError(f"{path}: metadados do BCB SGS: esperado um objeto JSON")

    if _RE_BASICO.match(path.name) and "basic" not in doc:
        basic, full = doc, None  # ``_basic.json`` é o próprio dicionário básico
    else:
        basic, full = doc.get("basic"), doc.get("full")
    if basic is None:
        return None
    if not isinstance(basic, dict):
        raise ValueError(f"{path}: 'basic' deveria ser um objeto")
    sid_dentro = _inteiro(basic.get("series_id"))
    if sid_dentro is not None and sid_dentro != sid_nome:
        raise ValueError(
            f"{path}: o arquivo é da série {sid_nome}, mas o metadado diz {sid_dentro}"
        )
    ultima = _data(full.get("last_update")) if isinstance(full, dict) else None
    return MetadadoSerie(
        series_id=sid_nome,
        name=_texto(basic.get("name")),
        name_abbreviated=_texto(basic.get("name_abbreviated")),
        name_english=_texto(basic.get("name_english")),
        name_english_abbreviated=_texto(basic.get("name_english_abbreviated")),
        theme_hierarchy=_temas(basic.get("theme_hierarchy"), path),
        frequency=_texto(basic.get("frequency")),
        unit=_texto(basic.get("unit")),
        source=_texto(basic.get("source")),
        start_date=_data(basic.get("start_date")),
        end_date=_data(basic.get("end_date")),
        series_type=_texto(basic.get("series_type")),
        precision=_inteiro(basic.get("precision")),
        min_value=_decimal(basic.get("min_value")),
        max_value=_decimal(basic.get("max_value")),
        manager_owner=_texto(basic.get("manager_owner")),
        special=_booleano(basic.get("special")),
        formula=_texto(basic.get("formula")),
        primitive_series=_texto(basic.get("primitive_series")),
        warning_message=_texto(basic.get("warning_message")),
        last_update=ultima,
    )
