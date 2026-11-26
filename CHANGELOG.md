# Changelog

Todas as alterações notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Não lançado]

### Corrigido
- `quantilica sql load bcb -s <pasta> --all` carrega a pasta do bruto do `bcb-sgs` (a raiz com `data/` ou a própria `data/`): só o snapshot mais recente de cada série (`quantilica.core.lake.select_current`, que entende o carimbo `@AAAAMMDDTHHMMSS` e o de só data), sem sidecars `.manifest.json`, `*.tmp` nem os metadados (`bcb-sgs_AAAA-MM/metadata/`), um arquivo por vez com seu registro no ledger. `--table <id>` (aceita `433`, `000433` e `series_433`) carrega uma série. Antes o adaptador abria o caminho como arquivo e a pasta falhava com `IsADirectoryError`; o `--all` era ignorado. Pasta sem `--all` nem `--table`, pasta sem arquivo de dados e `--table` sem correspondência saem com erro claro. Arquivo de série que não existe falha em vez de registrar zero linhas no ledger.
- O ledger do `bcb` usa `sha256` (`should_load_file`): arquivo igual é pulado, o mesmo nome com conteúdo novo recarrega, registro legado (sem hash) só ganha o hash. `--force` agora recarrega de verdade (antes era ignorado).
- Revisão de série: um segundo snapshot da mesma série não dá mais `UniqueViolation` em `uq_bcb_sgs_series_date`. A carga vai para uma tabela temporária e, na mesma transação do arquivo, desativa só o ponto ativo cujo valor (ou `date_end`) mudou e insere o que é novo ou revisado; ponto idêntico é no-op (recarga sem churn). Ponto repetido (mesma série e data) no mesmo arquivo vale o último, com aviso. O valor `0` numérico (`"value": 0`) deixa de ser perdido (`item.get("value") or item.get("valor")` tratava `0` como ausente); só `null` e a chave ausente são ausência. Números do JSON são lidos como `Decimal`, sem passar por `float`.
- `status` do `bcb` informa também `series_data_ativas`, `series_data_revisadas` (linhas desativadas por revisão) e `series_distintas`.
- `quantilica sql load sidra -s <pasta> --all` carrega a pasta do bruto do `ibge-sidra` (a raiz com `t-<id>/` ou uma pasta `t-<id>`): só a versão vigente de cada consulta (`quantilica.core.lake.select_current`, carimbo `@AAAAMMDD` mais novo), sem `metadados@...`, `indice_pesquisas@...`, sidecars nem `*.tmp`, um arquivo por vez com seu registro no ledger. Antes o adaptador abria o caminho como arquivo e a pasta falhava com `IsADirectoryError`; o `--all` era ignorado. Pasta sem `--all` nem `--table <id>` sai com erro claro (em vez de carregar tudo ou nada); pasta sem arquivo de dados também. O id da tabela vem de `t-<id>` no nome do arquivo ou da pasta.
- `ibge_sidra.dados.modificacao` passa a ser a data da fonte: o campo `modificacao` do item (se vier), senão a `metadata.modificacao` do sidecar `.manifest.json` do arquivo, senão o carimbo `@AAAAMMDD` do nome. Sem nenhuma (a API não informou: `0001-01-01` / `@00010101`), fica `NULL`. As respostas cruas do `/values` não trazem o campo e as linhas recebiam o valor fixo `2026-01-01`. A data de coleta (`fetched_at`, `metadados@...`) nunca entra. Data ilegível no item falha com erro claro. A coluna deixa de ser `NOT NULL DEFAULT CURRENT_DATE` (a data da carga não é a data da fonte); tabela criada antes é migrada na primeira carga (`DROP NOT NULL`, `DROP DEFAULT`).

### Adicionado
- `quantilica_sql.adapters._entrada.arquivos_vigentes`: resolvedor de entrada compartilhado pelos adaptadores que leem uma pasta do bruto (`sidra` e `bcb`). Dado o que é arquivo de dados e a chave da tabela/série, devolve só as versões vigentes, sem sidecars, `*.tmp` nem auxiliares, com erros claros. O `sidra` passa a usá-lo, sem mudança de comportamento.

## [0.3.1] - 2026-11-25

### Corrigido
- `quantilica sql lake convert` não entrega mais ao conversor todas as versões carimbadas do mesmo arquivo bruto (`nome@AAAAMMDD.ext`). Quando a pasta da fonte tem mais de uma versão de algum arquivo, o conversor recebe uma visão temporária só com a vigente (escolha de `quantilica.core.lake.select_current`, montada por `materialize_view` com hardlinks ou symlinks), removida ao final, também em erro. Fica em `QUANTILICA_LAKE_SCRATCH` ou, sem a variável, no diretório temporário do sistema (`TMPDIR`). Sem arquivo com mais de uma versão, o conversor recebe a pasta original. A visão nunca copia dados: se o scratch não aceita link a partir do bruto, o comando sai com erro claro, sugerindo apontar `QUANTILICA_LAKE_SCRATCH` para uma pasta do mesmo volume do bruto e fora da pasta do bruto, ou passar a pasta da visão vigente pronta (`quantilica lake select`); scratch dentro da pasta do bruto também sai com esse erro claro (código 1). As pastas de visão começam com ponto (`.quantilica-vigente-*`); as deixadas por um processo morto (o `owner.json` guarda o pid e o host) são removidas na próxima montagem, inclusive as do prefixo antigo, sem ponto. Conversor que escolhe a vigente por conta própria (por exemplo, pelo recurso do manifesto, como o `antt`) declara `selects_versions = True` no objeto do entry point `quantilica.lake` (também reconhecido em `functools.partial` e `__wrapped__`) e recebe a pasta original. Novo módulo `quantilica_sql.vigente`. Exige `quantilica-core>=0.15.0`.
- A sobreposição entre o scratch e a pasta da fonte é conferida explicitamente antes de montar a visão vigente; um `ValueError` de outra origem deixa de ser apresentado como "scratch dentro do bruto".
- CI: o `uv sync` do workflow de testes deixa de passar `--index` e `--index-strategy` do índice Quantilica. O pacote não depende de nada que só exista no índice, e o GitHub Pages responde com 503 intermitente quando o caminho não existe, o que derrubava o job.

## [0.3.0] - 2026-11-14

### Corrigido
- `ensure_schema` e o `TransformRunner` só executam `CREATE SCHEMA` quando o schema não existe (`pg_namespace`). `CREATE SCHEMA IF NOT EXISTS` exige o privilégio `CREATE` no banco mesmo com o schema existente, e o papel da aplicação em produção não o tem: os marts do Tesouro Direto falhavam com *permission denied* ao publicar.

### Adicionado
- `quantilica sql validate` confere as referências dentro do SQL: em `engine = "duckdb"`, toda view `fonte.alias` lida precisa constar em `lake_aliases.toml` (na raiz do catálogo ou em `tests/`). Referência inexistente vira erro. Sem `lake_aliases.toml` as views do lake não são conferidas, e o `engine = "postgres"` não é conferido (as tabelas podem vir da carga bruta).

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
