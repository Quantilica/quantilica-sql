"""Gerador de scaffolds para pipelines e catálogos declarativos."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

from quantilica_sql.validator import PipelineValidator

PipelineType = Literal["ibge", "bcb", "general"]


def _slugify(name: str) -> str:
    """Converte nome em slug válido para arquivos/diretórios."""
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9\-_]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_-")


def _ensure_dir(path: Path) -> Path:
    """Cria diretório se não existir."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def _write_if_not_exists(path: Path, content: str) -> None:
    """Escreve arquivo apenas se não existir (evita sobrescrever)."""
    if not path.exists():
        path.write_text(content, encoding="utf-8")


def _render_fetch_ibge(name: str) -> str:
    """Template fetch.toml para pipelines IBGE/SIDRA."""
    return f"""# Pipeline: {name}
# Fonte: IBGE SIDRA / Agregados API
# Referência: https://sidra.ibge.gov.br/tabelas

# Exemplo de tabela SIDRA - substitua pelos códigos desejados
[[tabelas]]
tabela_sidra = "1419"
variables = ["63", "66"]
territories = {{1 = [], 6 = [], 7 = []}}
classifications = {{315 = []}}
description = "IPCA - Variação e peso mensal (a partir de janeiro/2012)"
"""


def _render_fetch_bcb(name: str) -> str:
    """Template fetch.toml para pipelines BCB SGS."""
    return f"""# Pipeline: {name}
# Fonte: BCB SGS (Sistema Gerenciador de Séries Temporais)
# Referência: https://dadosabertos.bcb.gov.br/

# Exemplo de séries por IDs
[[series]]
ids = [432, 11, 12, 226]
description = "Taxas de juros: SELIC-META, SELIC-OVER, CDI, TR"

# Ou séries por tema
[[series]]
themes = ["Taxas de juros"]
description = "Todas as séries do tema 'Taxas de juros'"
"""


def _render_fetch_general(name: str) -> str:
    """Template fetch.toml genérico para pipelines cross-domain."""
    slug = _slugify(name)
    return f"""# Pipeline: {name}
# Fonte: Cross-domain (múltiplas origens: ibge_sidra, bcb_sgs, pdet, rfb_cnpj, etc.)

[pipeline]
name = "{slug}"
domains = ["ibge_sidra", "bcb_sgs"]
description = "Pipeline cross-domain integrando múltiplas fontes de dados"
"""


def _render_transform(name: str) -> str:
    """Template transform.toml com entrada [[table]] padrão."""
    slug = _slugify(name)
    return f"""[[table]]
name        = "{slug}"
schema      = "analytics"
strategy    = "replace"
sql         = "{slug}.sql"
description = "Tabela analítica gerada a partir de {name}"
"""


def _render_sql(name: str, pipeline_type: PipelineType) -> str:
    """Template SQL com query padrão por tipo de pipeline."""
    if pipeline_type == "ibge":
        return f"""-- Pipeline IBGE/SIDRA: {name}
-- Lê a superfície do schema ibge_sidra criado por `quantilica sql load sidra`
-- (dados, periodo, dimensao, localidade); o runner coloca o schema no search_path.
--   dados      uma linha por célula; `v` é texto (número ou símbolo do IBGE) e
--              `ativo` marca a versão vigente (consulta no tempo: dados_em(data))
--   periodo    data_inicio, ano, mes, trimestre, frequencia
--   localidade nc/nn = nível territorial, d1c/d1n = código e nome da localidade
--   dimensao   mc/mn = unidade, d2c/d2n = variável, d4c/d4n... = classificações,
--              na ordem das classificações da tabela
-- Wide/pivot: variáveis do SIDRA viram colunas. Substitua pelo SQL do seu pipeline.

SELECT
    p.data_inicio AS periodo,
    l.nc AS nivel_territorial_id,
    l.nn AS nivel_territorial,
    l.d1c AS localidade_id,
    l.d1n AS localidade,
    dim.d4c AS classificacao_id,
    dim.d4n AS classificacao,
    MAX(CASE WHEN dim.d2c = '63' AND d.v ~ '^-?[0-9]' THEN d.v::numeric END)::float8
        AS variavel_63,
    MAX(CASE WHEN dim.d2c = '66' AND d.v ~ '^-?[0-9]' THEN d.v::numeric END)::float8
        AS variavel_66
FROM dados d
JOIN periodo p ON d.periodo_id = p.id
JOIN dimensao dim ON d.dimensao_id = dim.id
JOIN localidade l ON d.localidade_id = l.id
WHERE d.tabela_sidra_id = '1419'
  AND d.ativo = TRUE
GROUP BY
    p.data_inicio,
    l.nc,
    l.nn,
    l.d1c,
    l.d1n,
    dim.d4c,
    dim.d4n
ORDER BY
    p.data_inicio,
    l.d1c,
    dim.d4c;
"""
    elif pipeline_type == "bcb":
        return f"""-- Pipeline BCB SGS: {name}
-- Query padrão para séries temporais do BCB

SELECT
    sd.date,
    COALESCE(sd.date_end, sd.date) AS date_end,
    CASE sd.series_id
        WHEN 432 THEN 'SELIC-META'
        WHEN 11 THEN 'SELIC-OVER'
        WHEN 12 THEN 'CDI'
        WHEN 226 THEN 'TR'
        ELSE sm.name
    END AS taxa,
    sm.name AS nome_serie,
    sm.unit AS unidade,
    sm.frequency AS frequencia,
    sd.value::double precision AS valor
FROM
    series_data sd
    INNER JOIN series_metadata sm ON sd.series_id = sm.series_id
WHERE
    sm.theme_hierarchy @> ARRAY['Taxas de juros']::text[]
    AND sd.ativo = TRUE
ORDER BY
    sd.date, date_end, taxa;
"""
    else:
        return f"""-- Pipeline Cross-Domain: {name}
-- Integração entre múltiplas fontes (analytics.*)
-- Substitua esta query pelo SQL específico do seu pipeline

WITH fonte_ibge AS (
    SELECT
        date,
        valor AS valor_ibge
    FROM analytics.exemplo_ibge
),
fonte_bcb AS (
    SELECT
        date,
        valor AS valor_bcb
    FROM analytics.exemplo_bcb
)
SELECT
    COALESCE(i.date, b.date) AS date,
    i.valor_ibge,
    b.valor_bcb
FROM fonte_ibge i
FULL OUTER JOIN fonte_bcb b ON i.date = b.date
ORDER BY date DESC;
"""


def _render_manifest(name: str, description: str) -> str:
    """Template manifest.toml com [catalog] e [pipelines.<id>]."""
    slug = _slugify(name)
    return f"""# Manifest consolidado de pipelines: {name}
# Gerado automaticamente por quantilica-sql scaffold

[catalog]
name = "{slug}"
version = "1.0.0"
description = "{description or f"Catálogo de pipelines: {name}"}"

[pipelines.{slug}]
theme = "general"
path = "main"
description = "Pipeline principal do catálogo {name}"
"""


def _render_readme(name: str, description: str) -> str:
    """Template README.md para o catálogo."""
    return f"""# {name}

{description or "Catálogo de pipelines declarativos para quantilica-sql."}

## Estrutura

```text
{name}/
├── manifest.toml          # Catálogo principal
├── {_slugify(name)}/        # Pipeline scaffolded
│   ├── fetch.toml         # Configuração de extração
│   ├── transform.toml     # Configuração de transformação
│   └── {_slugify(name)}.sql # Query SQL
```

## Uso

```bash
# Validar o catálogo
quantilica-sql plugin install <url-do-repositorio> --alias {_slugify(name)}

# Executar pipeline
quantilica-sql run {_slugify(name)}

# Ver status do banco
quantilica-sql status
```

## Desenvolvimento

1. Edite `fetch.toml` para definir as fontes de dados
2. Edite `{_slugify(name)}.sql` com a query analítica
3. Execute `quantilica-sql run {_slugify(name)}` para testar
4. Valide com `quantilica-sql plugin list`
"""


def scaffold_pipeline(
    target_dir: Path,
    name: str,
    pipeline_type: PipelineType = "general",
) -> Path:
    """Cria um novo pipeline com fetch.toml, transform.toml e .sql.

    Args:
        target_dir: Diretório onde criar o pipeline (ex: ./meus-pipelines)
        name: Nome do pipeline (será slugificado para o diretório)
        pipeline_type: Tipo de pipeline - "ibge", "bcb" ou "general"

    Returns:
        Path para o diretório do pipeline criado.

    Raises:
        ValueError: Se pipeline_type for inválido.
    """
    if pipeline_type not in ("ibge", "bcb", "general"):
        raise ValueError(
            f"pipeline_type inválido: {pipeline_type!r}."
            " Use 'ibge', 'bcb' ou 'general'."
        )

    slug = _slugify(name)
    pipeline_dir = _ensure_dir(target_dir / slug)

    # fetch.toml
    fetch_content = {
        "ibge": _render_fetch_ibge,
        "bcb": _render_fetch_bcb,
        "general": _render_fetch_general,
    }[pipeline_type](name)
    _write_if_not_exists(pipeline_dir / "fetch.toml", fetch_content)

    # transform.toml
    _write_if_not_exists(pipeline_dir / "transform.toml", _render_transform(name))

    # .sql
    _write_if_not_exists(pipeline_dir / f"{slug}.sql", _render_sql(name, pipeline_type))

    # Validação automática
    validator = PipelineValidator()
    report = validator.validate_pipeline_dir(pipeline_dir)
    if not report.ok:
        errors = "\n".join(f"  - {i.message}" for i in report.errors)
        raise ValueError(f"Pipeline scaffolded inválido:\n{errors}")

    return pipeline_dir


def scaffold_catalog(
    target_dir: Path,
    name: str,
    description: str = "",
) -> Path:
    """Cria um novo catálogo com manifest.toml, README.md e primeiro pipeline.

    Args:
        target_dir: Diretório onde criar o catálogo
        name: Nome do catálogo
        description: Descrição opcional do catálogo

    Returns:
        Path para o diretório do catálogo criado.
    """
    slug = _slugify(name)
    catalog_dir = _ensure_dir(target_dir / slug)

    # manifest.toml
    _write_if_not_exists(
        catalog_dir / "manifest.toml", _render_manifest(name, description)
    )

    # README.md
    _write_if_not_exists(catalog_dir / "README.md", _render_readme(name, description))

    # Primeiro pipeline scaffolded
    scaffold_pipeline(catalog_dir, "main", pipeline_type="general")

    # Validação completa do catálogo
    validator = PipelineValidator()
    report = validator.validate_catalog(catalog_dir)
    if not report.ok:
        errors = "\n".join(f"  - {i.message}" for i in report.errors)
        raise ValueError(f"Catálogo scaffolded inválido:\n{errors}")

    return catalog_dir
