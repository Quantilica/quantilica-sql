# quantilica-sql

Motor unificado de ingestão, particionamento e pipelines analíticos no PostgreSQL para o ecossistema Quantilica.

## Visão Geral

`quantilica-sql` substitui e unifica os antigos motores independentes `sidra-sql` e `bcb-sgs-sql`, fornecendo uma plataforma coesa para carregar dados brutos de fontes públicas brasileiras (APIs REST ou arquivos massivos como RAIS, RFB-CNPJ, CVM e INMET) em schemas dedicados do PostgreSQL, e materializar views e tabelas desnormalizadas na camada analítica (`analytics`).

## Instalação

Como plugin do `quantilica-cli`:
```bash
quantilica install sql
```

Ou diretamente no seu projeto Python (o pacote é distribuído pelo índice da Quantilica, não pelo PyPI):
```bash
uv add quantilica-sql --index https://index.quantilica.com/simple/
# ou com suporte a polars para leituras complexas:
uv add "quantilica-sql[analysis]" --index https://index.quantilica.com/simple/
```

## Requisitos

PostgreSQL 12 ou mais novo (o esquema do SIDRA usa colunas geradas `STORED`); os testes rodam no 16 e no 18. A DDL só pede `CREATE` no schema, nunca no banco: o papel da aplicação precisa de `USAGE` e `CREATE` no schema (ou ser o dono dele).

## Uso

### Execução via `quantilica-cli`
```bash
# Inspecionar status dos schemas e tabelas no PostgreSQL
quantilica sql status

# Executar um pipeline analítico declarativo
quantilica sql run macro/ipca

# Carga sob demanda de uma partição
quantilica sql load rfb --competencia 2023-10
quantilica sql load pdet -s ./data/pdet

# SIDRA: a raiz do bruto do ibge-sidra (t-<id>/...); só a versão vigente de cada consulta
quantilica sql load sidra -s ./data/ibge-sidra --all
quantilica sql load sidra -s ./data/ibge-sidra --table 1737        # uma tabela
quantilica sql load sidra -s ./data/ibge-sidra --all --historico   # todas as versões, em ordem de data
quantilica sql load sidra -s ./data/ibge-sidra --all --force       # recarrega mesmo com o mesmo sha256
quantilica sql load sidra -s ./data/ibge-sidra --all --recriar     # só para sair do esquema 0.3.x

# Gestão de catálogos e plugins
quantilica sql plugin list
quantilica sql plugin install https://github.com/Quantilica/quantilica-pipelines.git
```

### Execução standalone (scripts, agendadores e contêineres)
```bash
quantilica-sql status
quantilica-sql run macro/ipca
```

## Arquitetura de Schemas

- **Schemas por Fonte:** `anp`, `bcb_sgs`, `cvm`, `inmet`, `pdet`, `rfb_cnpj`, `ibge_sidra`, etc.
- **Camada Analítica / Data Marts:** `analytics`.
- **Rastreabilidade de Idempotência:** `{schema}.arquivo_carregado`.

## SIDRA (`ibge_sidra`)

Modelo estrela em formato longo. O papel de cada dimensão `D1..Dn` do arquivo vem da URL do sidecar `.manifest.json` e dos `metadados@AAAAMMDD.json` da tabela (nunca da posição: o SIDRA numera na ordem da URL); divergência ou falta de metadados é erro claro.

| Objeto | O que é |
|---|---|
| `observacao` | fato: uma linha por célula e versão (`valor` numérico exato ou `simbolo`: `-`, `..`, `...`, `X`, `Z`), `ativo` marca a vigente |
| `serie` + `serie_categoria` | variável, unidade e categorias das N classificações (a ponte fica na série, não no fato) |
| `tabela`, `variavel`, `classificacao`, `categoria`, `localidade`, `periodo` | dimensões (a categoria é por tabela) |
| `versao` | um arquivo = uma versão (`consulta`, `modificacao`, `sha256`) |
| `dados`, `dimensao` (views), `localidade`, `periodo` | superfície das consultas do catálogo (`dados.v` é o texto do valor ou do símbolo) |
| `observacao_plana` (view) | os nomes, com as categorias em JSONB, sem JOIN |
| `dados_em(data)` (função) | o que valia numa data: a versão de maior `modificacao` até ela em cada célula |

Uma revisão do IBGE (arquivo `@data_nova` da mesma consulta) vira versão nova; a antiga fica como história. Só uma observação por célula é `ativo`: a da maior `modificacao`. A consulta "o que valia em D" é `dados_em(D)`; `modificacao <= D AND ativo` descarta o que foi revisado depois de D e não serve.

## Lake de Parquet (extra `lake`)

Microdados volumosos (RAIS/CAGED, CNPJ, microdados do Tesouro Direto) não precisam morar no PostgreSQL: o banco guarda o que é servido, e o resto fica em arquivos Parquet consultados com DuckDB.

```bash
uv add "quantilica-sql[lake]" --index https://index.quantilica.com/simple/

# converte os arquivos brutos (CSV/TXT, ZIP, 7z) em Parquet, em streaming
quantilica sql lake convert pdet --source-dir ./data/pdet
quantilica sql lake status
```

- A pasta do lake vem de `QUANTILICA_LAKE_DIR` (padrão: `lake/` ao lado do diretório de dados). Mantenha-a **fora** da pasta de dados brutos.
- Um Parquet por arquivo de origem, em `<fonte>/<dataset>/<partição>/<chave>.parquet`. A chave é o nome sem a data de publicação (`@AAAAMMDD`): uma republicação substitui o Parquet anterior. O `sha256` de origem fica nos metadados do Parquet, e o arquivo só é reconvertido quando o hash muda.
- As colunas ficam como texto, com os nomes normalizados (sem acento, minúsculos). A tipagem é feita na consulta, com `TRY_CAST`.
- Pipelines com `engine = "duckdb"` no `transform.toml` (ou no `manifest.toml`) executam a consulta no DuckDB sobre as views do lake (`<fonte>.<dataset>`) e materializam o resultado no PostgreSQL com a mesma troca atômica da estratégia `replace`. Um `prelude` opcional cria views compartilhadas pelas tabelas do arquivo.
- `quantilica sql validate` confere manifest, `fetch.toml` e `transform.toml` sem precisar de banco.

