# Auditoria da Tabela `arquivo_carregado` por Schema

Relatório comparativo da estrutura da tabela `arquivo_carregado` nos schemas de domínio do quantilica-sql.

---

## Tabela Comparativa

| Schema | Colunas | PK | Total Registros | Observações |
|--------|---------|----|-----------------|-------------|
| rfb_cnpj | arquivo (text), competencia (character varying), tabela (text), linhas (bigint), carregado_em (timestamp with time zone) | arquivo, competencia, tabela | 1063 | Falta coluna sha256; Falta coluna particao; PK diferente do contrato unificado: ['arquivo', 'competencia', 'tabela'] |
| pdet | nome_arquivo (text), dataset (text), linhas (bigint), carregado_em (timestamp with time zone) | nome_arquivo | 1169 | Falta coluna sha256; Falta coluna particao; PK diferente do contrato unificado: ['nome_arquivo'] |
| cvm | arquivo (text), grupo (text), tabela (text), linhas (bigint), carregado_em (timestamp with time zone) | arquivo | 711 | Falta coluna sha256; Falta coluna particao; PK diferente do contrato unificado: ['arquivo'] |
| inmet | — | — | 0 | Tabela não existe |
| ibge_sidra | arquivo (text), tabela_sidra_id (text), carregado_em (timestamp with time zone) | arquivo | 4383 | Falta coluna sha256; Falta coluna particao; PK diferente do contrato unificado: ['arquivo'] |
| anp | nome_arquivo (text), dataset (text), linhas (bigint), carregado_em (timestamp with time zone) | nome_arquivo | 0 | Falta coluna sha256; Falta coluna particao; PK diferente do contrato unificado: ['nome_arquivo'] |
| bcb_sgs | — | — | 0 | Tabela não existe |

---

## Detalhes por Schema

### Schema: `rfb_cnpj`

#### Colunas

| Coluna | Tipo | Nullable | Default |
|--------|------|----------|---------|
| arquivo | text | NÃO | — |
| competencia | character varying | NÃO | — |
| tabela | text | NÃO | — |
| linhas | bigint | SIM | — |
| carregado_em | timestamp with time zone | SIM | clock_timestamp() |

**Chave Primária:** arquivo, competencia, tabela

**Total de Registros:** 1063

#### Amostra (até 3 registros)

| arquivo | competencia | tabela | linhas | carregado_em |
| --- | --- | --- | --- | --- |
| Cnaes@20250520.zip | 2023-05 | cnaes | 1359 | 2026-10-19 21:03:46.296991-03:00 |
| Motivos@20250520.zip | 2023-05 | motivos | 61 | 2026-10-19 21:03:46.297794-03:00 |
| Municipios@20250520.zip | 2023-05 | municipios | 5571 | 2026-10-19 21:03:46.298019-03:00 |

#### Observações

- Falta coluna sha256
- Falta coluna particao
- PK diferente do contrato unificado: ['arquivo', 'competencia', 'tabela']

### Schema: `pdet`

#### Colunas

| Coluna | Tipo | Nullable | Default |
|--------|------|----------|---------|
| nome_arquivo | text | NÃO | — |
| dataset | text | NÃO | — |
| linhas | bigint | NÃO | — |
| carregado_em | timestamp with time zone | SIM | now() |

**Chave Primária:** nome_arquivo

**Total de Registros:** 1169

#### Amostra (até 3 registros)

| nome_arquivo | dataset | linhas | carregado_em |
| --- | --- | --- | --- |
| caged_200701@20170821.7z | caged | 2182618 | 2026-10-17 10:52:29.107059-03:00 |
| rais-estabelecimentos_1985@20160719.7z | rais-estabelecimentos | 1088918 | 2026-10-17 10:52:29.107059-03:00 |
| caged_200702@20170821.7z | caged | 2127455 | 2026-10-17 10:53:47.430193-03:00 |

#### Observações

- Falta coluna sha256
- Falta coluna particao
- PK diferente do contrato unificado: ['nome_arquivo']

### Schema: `cvm`

#### Colunas

| Coluna | Tipo | Nullable | Default |
|--------|------|----------|---------|
| arquivo | text | NÃO | — |
| grupo | text | SIM | — |
| tabela | text | SIM | — |
| linhas | bigint | SIM | — |
| carregado_em | timestamp with time zone | SIM | clock_timestamp() |

**Chave Primária:** arquivo

**Total de Registros:** 711

#### Amostra (até 3 registros)

| arquivo | grupo | tabela | linhas | carregado_em |
| --- | --- | --- | --- | --- |
| cad_adm_cart@20261017.zip | adm-cart-cad | multi_table_zip | 26485 | 2026-10-18 16:11:43.622099-03:00 |
| cad_cia_aberta@20261017.csv | cia-cad | cad_cia_aberta | 2678 | 2026-10-18 16:17:29.882164-03:00 |
| cad_agente_fiduc@20261017.zip | agente-fiduc-cad | multi_table_zip | 118 | 2026-10-18 16:17:45.778177-03:00 |

#### Observações

- Falta coluna sha256
- Falta coluna particao
- PK diferente do contrato unificado: ['arquivo']

### Schema: `inmet`

**Tabela não existe.**

### Schema: `ibge_sidra`

#### Colunas

| Coluna | Tipo | Nullable | Default |
|--------|------|----------|---------|
| arquivo | text | NÃO | — |
| tabela_sidra_id | text | NÃO | — |
| carregado_em | timestamp with time zone | NÃO | now() |

**Chave Primária:** arquivo

**Total de Registros:** 4383

#### Amostra (até 3 registros)

| arquivo | tabela_sidra_id | carregado_em |
| --- | --- | --- |
| t-1686_p-198911_f-a_n1-all_n6-all_n7-all_v-44_c72-@2003-04-10.json | 1686 | 2026-05-22 21:43:03.164906-03:00 |
| t-1686_p-199002_f-a_n1-all_n6-all_n7-all_v-44_c72-@2002-08-16.json | 1686 | 2026-05-22 21:43:03.164906-03:00 |
| t-1686_p-198912_f-a_n1-all_n6-all_n7-all_v-44_c72-@2003-04-10.json | 1686 | 2026-05-22 21:43:03.164906-03:00 |

#### Observações

- Falta coluna sha256
- Falta coluna particao
- PK diferente do contrato unificado: ['arquivo']

### Schema: `anp`

#### Colunas

| Coluna | Tipo | Nullable | Default |
|--------|------|----------|---------|
| nome_arquivo | text | NÃO | — |
| dataset | text | NÃO | — |
| linhas | bigint | NÃO | — |
| carregado_em | timestamp with time zone | SIM | now() |

**Chave Primária:** nome_arquivo

**Total de Registros:** 0

#### Observações

- Falta coluna sha256
- Falta coluna particao
- PK diferente do contrato unificado: ['nome_arquivo']

### Schema: `bcb_sgs`

**Tabela não existe.**

---

## Proposta de Migração para Contrato Unificado

### Contrato Alvo

A tabela `arquivo_carregado` deve seguir o contrato unificado:

| Coluna | Tipo | Nullable | Descrição |
|--------|------|----------|-----------|
| dataset | TEXT | NOT NULL | Nome do dataset (ex.: caged, rais, sidra_123) |
| particao | TEXT | NOT NULL | Partição lógica (ex.: 2024-01, uf=SP) |
| nome_arquivo | TEXT | NOT NULL | Nome do arquivo original |
| sha256 | TEXT | NOT NULL | Hash SHA-256 do arquivo |
| carregado_em | TIMESTAMPTZ | NOT NULL | Timestamp de carregamento |
| tamanho_bytes | BIGINT | NOT NULL | Tamanho do arquivo em bytes |
| metadados | JSONB | NULL | Metadados adicionais |

**Chave Primária:** `(dataset, particao, nome_arquivo)`

### Estratégia de Migração Segura

1. **Criar nova tabela** com o contrato unificado em cada schema
2. **Migrar dados existentes** com `INSERT ... SELECT` mapeando colunas
3. **Validar integridade** (contagem, PKs, amostras)
4. **Renomear tabelas** (swap atômico com `ALTER TABLE ... RENAME TO`)
5. **Remover tabelas antigas** após validação em produção

### Exemplo de SQL de Migração (por schema)

```sql
-- 1. Criar nova tabela
CREATE TABLE {schema}.arquivo_carregado_new (
    dataset TEXT NOT NULL,
    particao TEXT NOT NULL,
    nome_arquivo TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT now(),
    tamanho_bytes BIGINT NOT NULL DEFAULT 0,
    metadados JSONB,
    PRIMARY KEY (dataset, particao, nome_arquivo)
);

-- 2. Migrar dados (exemplo genérico - ajustar por schema)
INSERT INTO {schema}.arquivo_carregado_new
    (dataset, particao, nome_arquivo, sha256, carregado_em, tamanho_bytes, metadados)
SELECT
    COALESCE(dataset, 'desconhecido') as dataset,
    COALESCE(particao, 'default') as particao,
    COALESCE(nome_arquivo, arquivo_nome) as nome_arquivo,
    COALESCE(sha256, '') as sha256,
    COALESCE(carregado_em, now()) as carregado_em,
    COALESCE(tamanho_bytes, 0) as tamanho_bytes,
    metadados
FROM {schema}.arquivo_carregado
ON CONFLICT (dataset, particao, nome_arquivo) DO NOTHING;

-- 3. Validar
SELECT COUNT(*) FROM {schema}.arquivo_carregado_new;

-- 4. Swap atômico
ALTER TABLE {schema}.arquivo_carregado RENAME TO arquivo_carregado_old;
ALTER TABLE {schema}.arquivo_carregado_new RENAME TO arquivo_carregado;

-- 5. Limpar (após validação)
DROP TABLE {schema}.arquivo_carregado_old;
```

> **Nota:** O mapeamento exato das colunas na etapa 2 depende da estrutura atual de cada schema.
> Schemas sem a tabela precisam apenas do `CREATE TABLE` (etapa 1).