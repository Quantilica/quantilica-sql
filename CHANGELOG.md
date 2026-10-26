# Changelog

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Unreleased]

### Adicionado
- Carga atômica por arquivo (`core.atomic.atomic_file_load`, `BaseSourceAdapter.atomic_file`): COPY, substituição da carga anterior e registro no ledger numa única transação. Falha em qualquer ponto desfaz tudo, sem linhas órfãs nem ledger adiantado.
- Os dados carregados passam a ser marcados com `_arquivo` (via tabela temporária) e o ledger grava as tabelas tocadas em `metadados._tabelas`; é isso que permite substituir uma carga, inclusive de ZIPs que alimentam várias tabelas.
- `BaseSourceAdapter.should_load_file`: compara o `sha256` do arquivo (sidecar `.manifest.json` ou cálculo) ao ledger. Arquivo republicado com o mesmo nome e outro hash é recarregado por substituição; registro legado (sem hash) só ganha o hash.
- `core.tracker.file_status` e `backfill_sha256`.
- `LegacyLoadError`: recarregar (`--force` ou hash novo) um arquivo cuja carga anterior não tem `_arquivo` falha em vez de duplicar linhas.
- Testes de integração com PostgreSQL real (`QUANTILICA_TEST_DATABASE_URL`; pulados sem ela).

### Alterado
- `quantilica-core>=0.12.0` (leitura do sidecar de manifesto).
- `copy_from_file/rows/stream` e o tracker não commitam mais dentro de `atomic_file_load`; fora dele o comportamento é o mesmo.
- INMET: o skip passou a consultar o ledger com a mesma partição (ano) com que registra (antes nunca encontrava o arquivo e recarregava sempre), e `_arquivo` passa a ser o arquivo de origem (o ZIP), não o membro.
- B3: `_arquivo` e o ledger usam o nome do arquivo original, não o do arquivo em staging.

### Corrigido
- ANP, ANAC, COMEX, DATASUS, RTN, Tesouro Direto, INEP e TSE carregavam a linha de cabeçalho do CSV como dado (`copy_from_file` não tinha `HEADER`). Adicionado o parâmetro `header`, usado por esses adaptadores. O defeito existe na 0.1.0/0.1.1.

### Não alterado
- PDET e RFB-CNPJ mantêm o contrato de carga anterior (ledger sem `sha256`, sem substituição): deixam de ser carregados no PostgreSQL (os microdados passam a ficar em arquivos Parquet) e seus adaptadores serão congelados.

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
