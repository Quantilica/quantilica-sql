# quantilica-sql

Motor unificado de ingestão, particionamento e pipelines analíticos no PostgreSQL para o ecossistema Quantilica.

## Visão Geral

`quantilica-sql` substitui e unifica os antigos motores independentes `sidra-sql` e `bcb-sgs-sql`, fornecendo uma plataforma coesa para carregar dados brutos de fontes públicas brasileiras (APIs REST ou arquivos massivos como RAIS, RFB-CNPJ, CVM e INMET) em schemas dedicados do PostgreSQL, e materializar views e tabelas desnormalizadas na camada analítica (`analytics`).

## Instalação

Como plugin do `quantilica-cli`:
```bash
quantilica install sql
```

Ou diretamente no seu ambiente Python:
```bash
uv add quantilica-sql
# ou com suporte a polars para leituras complexas:
uv add "quantilica-sql[analysis]"
```

## Uso

### Execução via `quantilica-cli`
```bash
# Inspecionar status dos schemas e tabelas no PostgreSQL
quantilica sql status

# Executar um pipeline analítico declarativo
quantilica sql run macro/ipca

# Carga sob demanda de uma partição
quantilica sql load rfb --competencia 2023-10
quantilica sql load pdet --datasets rais-vinculos

# Gestão de catálogos e plugins
quantilica sql plugin list
quantilica sql plugin install https://github.com/Quantilica/quantilica-pipelines.git
```

### Execução Standalone (para workers, cron e Docker)
```bash
quantilica-sql status
quantilica-sql run macro/ipca
```

## Arquitetura de Schemas

- **Schemas por Fonte:** `anp`, `bcb_sgs`, `cvm`, `inmet`, `pdet`, `rfb_cnpj`, `ibge_sidra`, etc.
- **Camada Analítica / Data Marts:** `analytics`.
- **Rastreabilidade de Idempotência:** `{schema}.arquivo_carregado`.
