# Contrato legado — `sidra-sql` e `bcb-sgs-sql` (M1.1)

Inventário formal dos contratos dos motores legados, base para a portabilidade
do runner em `quantilica_sql.runner`. Fontes lidas: `sidra_sql.{toml_runner,
transform_runner,database,validator,plugin_manager,runner,cli}`,
`bcb_sgs_sql.{loader,toml_runner,transform_runner,validator,database}` e
amostras de `quantilica-pipelines/` (`macro/juros`, `ibge/agropecuaria/abate`,
`cross/macro_trabalho`).

Convenção: "ambos" = `sidra-sql` e `bcb-sgs-sql`. Os dois `TransformRunner` e
os dois `PluginValidator` são **idênticos em comportamento** (só mudam
docstrings e mensagens de erro), então o contrato é único.

---

## 1. `transform.toml` — seções e chaves

Arquivo vive no diretório do pipeline; cada entrada aponta para um `.sql`
**relativo ao diretório do TOML** (`toml_path.parent / sql`).

| Chave | Obrigatória | Tipo | Semântica |
|---|---|---|---|
| `[[table]]` | sim (≥1) | array de tabelas | Uma saída por entrada. `[table]` singular (dict) é **rejeitado** (erro explícito no sidra-sql; no bcb-sgs-sql só falha por não ser lista). |
| `name` | sim | str | Nome do objeto de saída. |
| `schema` | sim | str | Schema de destino. Todas as amostras usam `analytics`. |
| `strategy` | sim | str | `"replace"` ou `"view"`. Qualquer outro valor → `ValueError` em runtime / erro no validator. |
| `sql` | sim | str | Caminho relativo do arquivo `.sql` (contém um `SELECT`). Pode ser compartilhado por nome (`transform.sql`) ou específico (`juros.sql`). |
| `description` | não | str | Apenas documentação; **ignorada** pelo runner. |
| `primary_key` | não | list[str] | Só `replace`. Gera `ALTER TABLE … ADD PRIMARY KEY ("c1", "c2")`. Ignorada em `view`. |
| `indexes` | não | list[table] | Só `replace`. Cada item: `name` (str, obrigatório), `columns` (list[str], obrigatório), `unique` (bool, opcional). Gera `CREATE [UNIQUE] INDEX "name" ON … ("c1", …)`. Ignorada em `view`. |

Observações verificadas:

- Chaves desconhecidas são ignoradas silenciosamente (sem validação estrita).
- O validator checa: TOML válido, `[[table]]` array não vazio, campos
  obrigatórios, `strategy ∈ {replace, view}`, **duplicidade de `(schema, name)`**
  e existência do arquivo `sql`. **Não** valida `primary_key`/`indexes`.
- Em amostras reais: `strategy = "replace"` em ~68 entradas e `"view"` em 1
  (`cross/macro_trabalho`); nenhuma amostra usa `indexes`; `primary_key` aparece
  (ex.: `["date", "date_end", "taxa"]` em `macro/juros`).
- Identificadores são citados com `"` mas **não escapados** (aspas internas em
  `name`/`schema`/colunas quebrariam o DDL — interpolação direta de string).
- O SQL lido recebe `.strip()` e `%` → `%%` (necessário porque
  `exec_driver_sql` do SQLAlchemy/psycopg trata `%` como placeholder). Um
  runner baseado em `psycopg` direto **sem parâmetros** não deve duplicar `%`.

## 2. Catálogo de estratégias

Só existem **duas** estratégias implementadas: `replace` e `view`. `pivot` e
`table` **não são estratégias**: "pivot/wide" é um padrão de escrita do SQL
(skill `sidra-transform`), e `table` seria sinônimo de `replace` mas é
rejeitado.

Todas as entradas executam em sequência (ordem do TOML), cada uma em **sua
própria transação** (`engine.begin()`); falha em uma entrada faz rollback só
dela — saídas anteriores do mesmo pipeline permanecem, as seguintes não rodam
(exceção propaga). Primeiro statement de cada transação:
`CREATE SCHEMA IF NOT EXISTS "<schema>"`.

### `replace`
```
DROP TABLE IF EXISTS "schema"."name";
CREATE TABLE "schema"."name" AS
<query>;
[ALTER TABLE … ADD PRIMARY KEY (…);]
[CREATE [UNIQUE] INDEX "…" ON … (…);]  -- um por item de indexes
```
- DDL transacional do PostgreSQL ⇒ o conjunto DROP+CREATE+PK+índices é
  **atômico**: leitores concorrentes veem a tabela antiga até o COMMIT (o
  `DROP` toma `ACCESS EXCLUSIVE` e bloqueia leitores durante a transação).
- Não é "swap" por tabela temporária/rename: a atomicidade vem apenas da
  transação. Se `primary_key` falhar (duplicatas/NULL), a tabela antiga é
  preservada (rollback).
- Falha de dependência: `DROP TABLE` sem `CASCADE` falha se houver views
  dependentes (ex.: a view cross-domain sobre `analytics.precos`). Não há
  tratamento; o erro sobe.
- Sem DML incremental: **full refresh** sempre.

### `view`
```
CREATE OR REPLACE VIEW "schema"."name" AS
<query>;
```
- Sem materialização, sem PK/índices. `CREATE OR REPLACE VIEW` exige que a
  nova definição mantenha as colunas existentes (mesmos nomes/tipos/ordem, só
  pode **acrescentar** ao final); caso contrário falha.
- Se a view substitui um objeto de outro tipo (tabela), falha.

### Arquivos `.sql` com DDL próprio (desvio do contrato)
`cross/macro_trabalho/transform.sql` contém `DROP VIEW IF EXISTS …;` e
`CREATE OR REPLACE VIEW analytics.macro_trabalho_indicadores AS …` **dentro do
próprio arquivo**, e o `transform.toml` também declara `strategy="view"`. O
runner legado embrulha o conteúdo em `CREATE OR REPLACE VIEW … AS\n<query>`,
o que produz SQL inválido com esse arquivo (múltiplos statements/`DROP`
dentro de um `AS`). **Marcar como inconsistência a resolver na portabilidade**
(corrigir o SQL da amostra ou suportar scripts livres explicitamente — ver §5).

## 3. `search_path` e schemas de origem vs. destino

- `get_engine` (ambos) cria o engine SQLAlchemy `postgresql+psycopg` com
  `connect_args={"options": "-c search_path=<config.db_schema>"}`.
  `config.db_schema` vem de `config.ini` → `[database] schema`.
- Schema **de origem (raw)** = `db_schema` do config:
  - `sidra-sql` → `ibge_sidra` (default documentado). Tabelas lidas sem
    qualificação: `dados`, `dimensao`, `localidade`, `tabela_sidra`.
  - `bcb-sgs-sql` → `bcb_sgs`. Tabelas: `series_data` (coluna `ativo`),
    `series_metadata` (`theme_hierarchy` como array), `theme`.
- Schema **de destino** = campo `schema` de cada `[[table]]` (todas as
  amostras: `analytics`), criado sob demanda. **Não** entra no `search_path`
  do engine: o `search_path` contém apenas o schema raw.
- Consequências:
  1. SQL de pipeline de **fonte única** usa nomes **não qualificados**
     (`FROM series_data sd JOIN series_metadata sm`), resolvidos pelo
     `search_path` da fonte.
  2. SQL **cross-domain** usa nomes **qualificados** (`analytics.precos`,
     `analytics.juros`, `analytics.pnadcm_ocupada`) — lê a camada analytics,
     não os raws; por isso não depende do `search_path`.
  3. O mesmo `transform.toml` rodado com `schema` raw trocado no config
     muda a origem sem mudar o SQL (acoplamento implícito ao config do
     motor). Pipelines cross-source (BCB + SIDRA no mesmo SELECT) só
     funcionam via analytics ou com qualificação explícita.
- O `quantilica_sql.transform_runner` atual diverge: usa `psycopg` direto,
  `SET search_path = "<analytics>", public` (destino, **não** o raw) e
  substitui `{{ target_schema }}`. Qualquer SQL legado com nomes raw
  não qualificados **não resolve** nesse modelo (ver §5).
- Config do `quantilica-sql`: `analytics_schema` via
  `QUANTILICA_ANALYTICS_SCHEMA` (default `analytics`); não há ainda conceito
  de schema raw por fonte.

## 4. Manifestos, `fetch.toml` e plugins

### 4.1 `fetch.toml` (por pipeline)
- **SIDRA**: array `[[tabelas]]`; obrigatório `tabela_sidra`. Demais chaves
  repassadas a `Fetcher.plan_periods`: `variables`, `territories`
  (`{nivel = [...]}`), `classifications`, e flags de expansão
  `unnest_classifications` (`true` ou lista de IDs) e `split_variables`
  (`unnest` tem precedência sobre `split`).
- **BCB SGS**: array `[[series]]`; cada seletor precisa de `ids` (lista de
  int) **ou** `themes` (lista de str; exige catálogo populado), com
  `frequency` opcional (`D/S/M/T/Qd/A`) estreitando `themes`.
- Fluxo do `fetch` (fora do escopo do porte de SQL, mas é o contrato de
  carga): schema/tabelas ORM → metadados → download paralelo → carga com
  controle de arquivos já carregados (`arquivo_carregado`, `--force-load`;
  BCB também com TTL de cache e desativação de observações antigas via
  `ativo`).
- Pipelines **cross-domain** (`cross/macro_trabalho/fetch.toml`) usam um
  bloco `[pipeline]` (`name`, `domains`, `description`) **sem**
  `[[tabelas]]`/`[[series]]`. O validator legado reporta erro para esse
  `fetch.toml` ("nenhuma [[tabelas]] declarada" / "[[series]]"); é o único
  caso observado de fetch.toml puramente declarativo — tratar explicitamente.

### 4.2 Manifesto do catálogo — duas formas divergentes
- **Esperada pelo código legado** (`PluginValidator` e
  `plugin_manager.read_manifest`): topo com `name`, `description`,
  `version` (ausência = *warning*), e array `[[pipeline]]` com `id`
  (obrigatório, único), `path` (obrigatório, relativo à raiz do plugin) e
  `description` opcional. Cada `path` deve conter `fetch.toml` e/ou
  `transform.toml` (ao menos um).
- **Real em `quantilica-pipelines/manifest.toml`**: `[catalog]`
  (`name`, `version`, `description`) + tabelas nomeadas
  `[pipelines.<id>]` com `theme`, `path`, `description`. **Esse formato não
  é lido pelo código legado** (que procuraria `[[pipeline]]`). O porte deve
  suportar o formato real (e opcionalmente o legado) ou o catálogo precisa
  ser migrado; decisão pendente.

### 4.3 Execução e descoberta
- Plugins são **repositórios git clonados**, não entry points Python:
  - sidra-sql: registro JSON (`registry.json`) em
    `user_config_dir("quantilica")/sidra-sql/`, clones em
    `user_data_dir(...)/sidra-sql/plugins/<alias>` (default `std` →
    `sidra-pipelines.git`; `install` falha se alias existir; `update` =
    `git pull`).
  - `quantilica_sql.plugin_manager.PluginManager`: sem registro; lista
    qualquer subdir de `plugins_dir` com `manifest.toml`; `install` faz
    `git pull --ff-only` se já existe (comportamento diferente do legado).
- **Entry points**: os motores legados **não declaram** nenhum
  (`[project.scripts]` apenas: `sidra-sql`, `bcb-sgs-sql`). O
  `quantilica-sql` declara `quantilica.commands` → `sql =
  quantilica_sql.cli:app` e script `quantilica-sql` (integração com o host
  `quantilica-cli`). Não existe grupo de entry point para plugins de
  pipeline.
- Ordem (`runner.run_subtree`): para cada diretório, **filhos primeiro**
  (`sorted(iterdir)` filtrando `_is_pipeline_dir`), depois o próprio
  `fetch.toml` (ETL), depois o `transform.toml`. Comandos `run <alias>
  [pipeline_id]` (todos se omitido), `run-path <dir>`, `transform <alias>
  <id>` (só transform, sem recursão).
- O `quantilica_sql.runner` atual **não** segue esse contrato: descobre
  pipeline por `*.sql` no diretório (`glob`), executa **todos** os `.sql`
  em ordem arbitrária de glob, ignora `transform.toml`/`fetch.toml`, sem
  estratégia, PK, índices, recursão nem manifesto.

## 5. Diretrizes de portabilidade para `quantilica_sql.runner`

1. **`transform.toml` é a fonte de verdade.** Carregar com `tomllib`,
   exigir `[[table]]` array, campos `name/schema/strategy/sql`; manter
   `description/primary_key/indexes`. Substituir o `glob("*.sql")` atual.
2. **Estratégias**: implementar exatamente `replace` e `view` com a mesma
   sequência de DDL do §2; erro explícito para estratégia desconhecida.
   `table`/`pivot` só se forem formalizadas (ADR) — hoje não existem.
3. **Transação por entrada**, com `CREATE SCHEMA IF NOT EXISTS` dentro dela.
   Com `psycopg` direto: `with conn.transaction():` por entrada (não um
   único autocommit nem `commit` global). Preservar semântica "falha
   preserva saídas anteriores".
4. **Atomicidade do `replace`**: manter DROP+CREATE+PK+índices na mesma
   transação. Melhoria opcional (fora do contrato legado): criar em tabela
   temporária e `RENAME` para reduzir bloqueio de leitores — registrar como
   decisão em ADR se adotada; não alterar silenciosamente.
5. **Identificadores**: usar `psycopg.sql.Identifier` (corrige a falta de
   escape do legado) e `sql.SQL(...)` para o corpo da query. Não duplicar
   `%` ao executar sem parâmetros; se usar parâmetros, escapar `%`.
6. **`search_path`**: o legado fixa o **schema raw** da fonte na conexão e
   escreve o destino sempre qualificado. O porte deve:
   - aceitar `raw_schema` por pipeline/fonte (ex.: `ibge_sidra`, `bcb_sgs`)
     — via config/manifesto — e aplicar
     `SET LOCAL search_path = "<raw>"` (opcionalmente `, public`) dentro da
     transação, **sem** trocar para o schema de destino;
   - manter `schema` do `[[table]]` como destino (default `analytics`
     de `analytics_schema` só como fallback se o TOML omitir — o legado
     exige o campo);
   - o placeholder `{{ target_schema }}` do runner atual não existe no
     legado; se mantido, tratá-lo como extensão opt-in.
7. **Manifestos**: ler `[catalog]`/`[pipelines.<id>]` (formato real) e, por
   compatibilidade, `[[pipeline]]` (`id`, `path`). Resolver `path` relativo
   à raiz do catálogo. Preservar a ordem **filhos → fetch → transform** do
   `run_subtree` e as entradas de CLI `run`, `run-path`, `transform`.
8. **Validator**: portar `PluginValidator` (idêntico nos dois legados),
   unificando os dois ramos de `fetch.toml` (`[[tabelas]]` vs `[[series]]`)
   e tratando o bloco `[pipeline]` cross-domain como válido. Manter
   `Severity/Issue/SectionReport/ValidationReport` e as regras do §1
   (inclui detecção de `(schema, name)` duplicado e SQL inexistente).
   Sugestão: validar também `primary_key`/`indexes` e chaves desconhecidas
   (*warning*).
9. **Cross-domain / DDL embutido**: decidir entre (a) corrigir
   `cross/macro_trabalho/transform.sql` para conter apenas o `SELECT`
   (alinha ao contrato) ou (b) introduzir estratégia explícita
   `script` para SQL livre. Recomendação: (a), mais barato e consistente
   com o contrato atual.
10. **Ordem de dependências**: `replace` sem `CASCADE` quebra ao recriar
    tabelas das quais views dependem; como `cross/*` são views sobre
    `analytics.*`, executar cross por último (a ordem filhos→pai e a ordem
    do manifesto já fazem isso hoje de forma implícita) e documentar.
11. **Plugins**: manter plugins como diretórios/repos git (sem entry
    points de pipeline). Reutilizar o `PluginManager` do `quantilica-sql`,
    acrescentando leitura de manifesto; decidir se haverá registro
    (`registry.json`) ou só varredura de `manifest.toml`.
12. **Testes de contrato (golden)**: cobrir ao menos — `replace` com e sem
    PK/índices; `view`; estratégia inválida; `[table]` singular; SQL
    ausente; rollback preserva saída anterior; `%` literal no SQL;
    `search_path` raw resolvendo nomes não qualificados; manifesto nos dois
    formatos.

### Lacunas conhecidas deste inventário
- Não foi executado nada contra banco; o contrato descreve o código, não
  comportamento observado em PostgreSQL.
- `database.py`/`loader.py` foram inspecionados só no que toca engine,
  `search_path` e fluxo de carga; o DDL/ORM completo das tabelas raw e as
  rotinas de upsert não estão detalhados aqui (fora do escopo de
  transform).
- Contagem de estratégias nas amostras vem de `grep` sobre `transform.toml`
  do repositório de pipelines no momento da leitura.
