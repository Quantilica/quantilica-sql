"""Especificações declarativas dos datasets que vivem no lake (Parquet).

O lake guarda o texto original, com os nomes de coluna normalizados e sem tipagem
(``VARCHAR``): a tipagem é feita nos marts, com ``TRY_CAST``, onde o erro é visível.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

SPEC_VERSION = 1
"""Muda quando a forma do Parquet muda; força a reconversão dos arquivos."""


@dataclass(frozen=True)
class DatasetSpec:
    source: str
    dataset: str
    separator: str = ";"
    encoding: str = "utf-8"
    header: bool = True
    # Obrigatório quando ``header`` é falso: nomes das colunas na ordem do arquivo.
    names: tuple[str, ...] = ()
    # Renomeia colunas já normalizadas (aliases entre publicações do mesmo dataset).
    rename: dict[str, str] = field(default_factory=dict)
    # Nome curto da view no DuckDB (padrão: o dataset normalizado).
    alias: str | None = None
    # Colunas (já normalizadas) que o cabeçalho precisa ter; protege contra a fonte
    # devolver uma página de erro no lugar do CSV.
    required: tuple[str, ...] = ()

    @property
    def view_name(self) -> str:
        return self.alias or normalize_name(self.dataset)


def normalize_name(name: str) -> str:
    """``Código do Investidor`` → ``codigo_do_investidor`` (sem acento, minúsculo)."""
    text = unicodedata.normalize("NFKD", name.lstrip("﻿"))
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^0-9a-zA-Z]+", "_", text).strip("_").lower()
    return text or "col"


def unique_names(names: list[str]) -> list[str]:
    """Normaliza e desambigua duplicatas (``col``, ``col_2``…)."""
    seen: dict[str, int] = {}
    out = []
    for raw in names:
        base = normalize_name(raw)
        seen[base] = seen.get(base, 0) + 1
        out.append(base if seen[base] == 1 else f"{base}_{seen[base]}")
    return out


# --- RFB-CNPJ: arquivos sem cabeçalho, layout oficial da Receita -----------------

RFB_TABLES: dict[str, tuple[str, ...]] = {
    "cnaes": ("codigo", "descricao"),
    "motivos": ("codigo", "descricao"),
    "municipios": ("codigo", "descricao"),
    "naturezas": ("codigo", "descricao"),
    "paises": ("codigo", "descricao"),
    "qualificacoes": ("codigo", "descricao"),
    "simples": (
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ),
    "socios": (
        "cnpj_basico",
        "identificador_socio",
        "nome_socio",
        "cnpj_cpf_socio",
        "qualificacao_socio",
        "data_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante",
        "faixa_etaria",
    ),
    "empresas": (
        "cnpj_basico",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo",
    ),
    "estabelecimentos": (
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ),
}

# --- PDET: cabeçalho no arquivo, latin-1 ----------------------------------------

PDET_DATASETS = (
    "rais-vinculos",
    "rais-estabelecimentos",
    "caged",
    "caged-2020-exc",
    "caged-2020-for",
    "caged-2020-mov",
    "caged-ajustes",
)

# --- Tesouro Direto: só os microdados (o resto continua no Postgres) ---------------

_TD_INVESTIDORES_RENAME = {"idade": "idade_do_investidor"}


def _build() -> dict[tuple[str, str], DatasetSpec]:
    specs: dict[tuple[str, str], DatasetSpec] = {}
    for name, cols in RFB_TABLES.items():
        specs[("rfb-cnpj", name)] = DatasetSpec(
            "rfb-cnpj", name, encoding="latin-1", header=False, names=cols
        )
    for name in PDET_DATASETS:
        specs[("pdet", name)] = DatasetSpec("pdet", name, encoding="latin-1")
    specs[("tesouro-direto", "investidores-do-tesouro-direto")] = DatasetSpec(
        "tesouro-direto",
        "investidores-do-tesouro-direto",
        rename=_TD_INVESTIDORES_RENAME,
        alias="investidores",
        required=("codigo_do_investidor", "data_de_adesao", "idade_do_investidor"),
    )
    specs[("tesouro-direto", "operacoes-do-tesouro-direto")] = DatasetSpec(
        "tesouro-direto",
        "operacoes-do-tesouro-direto",
        alias="operacoes",
        required=("codigo_do_investidor", "data_da_operacao", "valor_da_operacao"),
    )
    return specs


SPECS: dict[tuple[str, str], DatasetSpec] = _build()


def get_spec(source: str, dataset: str) -> DatasetSpec | None:
    return SPECS.get((source, dataset.lower()))


def sources() -> list[str]:
    return sorted({s for s, _ in SPECS})
