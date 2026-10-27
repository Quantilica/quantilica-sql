# Changelog

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [0.2.0] - 2026-10-27

### Adicionado
- Carga atômica por arquivo (`core.atomic.atomic_file_load`, `BaseSourceAdapter.atomic_file`): COPY, substituição da carga anterior e registro no ledger numa única transação. Falha em qualquer ponto desfaz tudo, sem linhas órfãs nem ledger adiantado.
- Os dados carregados passam a ser marcados com `_arquivo` (via tabela temporária) e o ledger grava as tabelas tocadas em `metadados._tabelas`; é isso que permite substituir uma carga, inclusive de ZIPs que alimentam várias tabelas.
- `BaseSourceAdapter.should_load_file`: compara o `sha256` do arquivo (sidecar `.manifest.json` ou cálculo) ao ledger. Arquivo republicado com o mesmo nome e outro hash é recarregado por substituição; registro legado (sem hash) só ganha o hash.
- `core.tracker.file_status` e `backfill_sha256`.
- `LegacyLoadError`: recarregar (`--force` ou hash novo) um arquivo cuja carga anterior não tem `_arquivo` falha em vez de duplicar linhas.
- Extra `lake` (`duckdb`) e módulo `quantilica_sql.lake`: o motor DuckDB lê o lake Parquet particionado no estilo Hive pelos descritores `_dataset.json` (`quantilica.core.lake`). Cada dataset vira a view `<fonte>.<alias>` sobre `read_parquet('<dataset>/**/*.parquet', hive_partitioning = true, union_by_name = true, filename = true)`, com os tipos de partição do descritor (chave sem tipo declarado fica `VARCHAR`, preservando zeros à esquerda). As colunas de partição viram colunas da view, e `_arquivo` traz o nome do Parquet de origem. Sem `_dataset.json`, a view usa o nome do dataset normalizado; descritor ilegível levanta `LakeError`.
- Engine `duckdb` no `transform.toml` (por tabela) e no `manifest.toml` (por pipeline): a consulta roda sobre as views do lake e o resultado é materializado no PostgreSQL por COPY, com a troca atômica do `replace`; tipos mapeados do DuckDB, NULL distinto de texto vazio. `prelude` opcional com views compartilhadas. Num ensaio com `target_schema`, o `search_path` resolve o schema de destino antes do schema de origem.
- `quantilica sql lake status`: arquivos, linhas e tamanho de cada dataset do lake (`QUANTILICA_LAKE_DIR`).
- `quantilica sql lake convert <fonte>`: atalho para o conversor do fetcher, encontrado pelo entry point `quantilica.lake`; mostra convertidos, pulados e erros e sai com código 1 se houver erro ou se a fonte não tiver conversor instalado (a mensagem orienta `quantilica install <fonte> --extra analysis`). Não exige o extra `lake`.
- `quantilica sql compare <schema_a> <schema_b> [-t tabela]`: compara tabela a tabela (colunas, contagem e `EXCEPT ALL` nos dois sentidos) e sai com código 1 se houver diferença. Serve de gate antes de trocar tabelas por versões recalculadas.
- `quantilica sql validate [caminho] [--strict]`: valida manifest, `fetch.toml` e `transform.toml` sem banco. Erro quando um `transform.toml` não é coberto por nenhum `path` do manifest ou quando o `engine` de uma entrada não é `postgres` nem `duckdb`.
- Testes de integração com PostgreSQL real (`QUANTILICA_TEST_DATABASE_URL`; pulados sem ela).

### Alterado
- Estratégia `replace` do `transform.toml`: a tabela nova é construída (dados, PK e índices) sem lock no destino e trocada por `RENAME` numa janela curta; antes, `DROP TABLE` + `CREATE TABLE AS` seguravam `ACCESS EXCLUSIVE` durante todo o rebuild. Views dependentes (inclusive em cadeia) são recriadas na troca, o que antes quebrava com "other objects depend on it". Falha na reconstrução mantém a tabela anterior. A troca aceita substituir uma materialized view (ou view) existente pela tabela nova; materialized view dependente falha com erro explícito.
- Estratégia `view` não usa mais `DROP VIEW … CASCADE`: view com dependentes dá erro em vez de derrubá-los em silêncio.
- `copy_from_file/rows/stream` e o tracker não commitam mais dentro de `atomic_file_load`; fora dele o comportamento é o mesmo.
- INMET: o skip passou a consultar o ledger com a mesma partição (ano) com que registra (antes nunca encontrava o arquivo e recarregava sempre), e `_arquivo` passa a ser o arquivo de origem (o ZIP), não o membro.
- B3: `_arquivo` e o ledger usam o nome do arquivo original, não o do arquivo em staging.
- Dependência `quantilica-core>=0.13.0` (contrato do lake e leitura do sidecar de manifesto).

### Descontinuado
- Adaptadores `pdet`, `rfb` e `tesouro_direto`: os microdados saem do PostgreSQL e passam a viver no lake Parquet. `get_adapter` emite `FutureWarning`, o contrato de carga deles não será mais evoluído (seguem sem `sha256` no ledger e sem substituição).
- Adaptadores `rtn` e `datasus`: ficam congelados.

### Corrigido
- ANP, ANAC, COMEX, DATASUS, RTN, Tesouro Direto, INEP e TSE carregavam a linha de cabeçalho do CSV como dado (`copy_from_file` não tinha `HEADER`). Adicionado o parâmetro `header`, usado por esses adaptadores. O defeito existe na 0.1.0/0.1.1.

## [0.1.1] - 2026-10-25

### Corrigido
- `quantilica sql` e o import de `quantilica_sql.adapters` falhavam em uma instalação sem o extra `analysis` (`No module named 'polars'`): o adaptador da RFB-CNPJ importava `polars` no topo do módulo. O import passou a ser tardio, só na carga de ZIP que precisa dele. A versão 0.1.0 tem esse defeito.

## [0.1.0] - 2026-10-25

### Adicionado
- Unificação dos motores `sidra-sql` e `bcb-sgs-sql` sob arquitetura modular de adaptadores.
- Subcomando `quantilica sql` integrado nativamente ao `quantilica-cli` via entry point `quantilica.commands`.
- Script de execução standalone `quantilica-sql`.
- Módulo `core` com streaming `COPY` binário/texto via `psycopg3`, particionamento declarativo e tabelas de idempotência (`arquivo_carregado`).
- Adaptadores para as 15 fontes públicas do ecossistema: ANAC, ANP, B3, BCB-SGS, Comex, CVM, DATASUS, INEP, INMET, PDET, RFB-CNPJ, RTN, SIDRA, Tesouro Direto e TSE, sobre um motor de ingestão genérico.
- Staging local com retomada incremental na carga da RFB-CNPJ.
- Executor de transformações SQL para materialização de data marts no schema `analytics`.
