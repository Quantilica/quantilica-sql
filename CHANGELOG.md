# Changelog

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

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
