-- Migration 001: Unify arquivo_carregado schema across all schemas
-- Target contract:
-- nome_arquivo TEXT NOT NULL
-- dataset TEXT NOT NULL
-- particao TEXT NOT NULL DEFAULT ''
-- sha256 TEXT
-- linhas BIGINT NOT NULL DEFAULT 0
-- carregado_em TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
-- metadados JSONB NOT NULL DEFAULT '{}'::jsonb
-- PRIMARY KEY (dataset, particao, nome_arquivo)
-- INDEX idx_<schema>_arquivo_sha256 (sha256)

-- ============================================================
-- SCHEMA: rfb_cnpj
-- Rename 'arquivo'->'nome_arquivo', add dataset default 'rfb_cnpj',
-- add particao from 'competencia', add sha256, add metadados, recreate PK
-- ============================================================
BEGIN;

-- Create new table with canonical schema
CREATE TABLE rfb_cnpj.arquivo_carregado_new (
    nome_arquivo TEXT NOT NULL,
    dataset TEXT NOT NULL DEFAULT 'rfb_cnpj',
    particao TEXT NOT NULL DEFAULT '',
    sha256 TEXT,
    linhas BIGINT NOT NULL DEFAULT 0,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    metadados JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (dataset, particao, nome_arquivo)
);

-- Migrate data: arquivo -> nome_arquivo, competencia -> particao, dataset = 'rfb_cnpj'
INSERT INTO rfb_cnpj.arquivo_carregado_new (nome_arquivo, dataset, particao, linhas, carregado_em)
SELECT arquivo, 'rfb_cnpj', competencia, COALESCE(linhas, 0), carregado_em
FROM rfb_cnpj.arquivo_carregado;

-- Verify row count
DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT count(*) INTO old_count FROM rfb_cnpj.arquivo_carregado;
    SELECT count(*) INTO new_count FROM rfb_cnpj.arquivo_carregado_new;
    IF old_count != new_count THEN
        RAISE EXCEPTION 'Row count mismatch for rfb_cnpj: old=%, new=%', old_count, new_count;
    END IF;
    RAISE NOTICE 'rfb_cnpj: migrated % rows', new_count;
END $$;

-- Drop old table and rename new
DROP TABLE rfb_cnpj.arquivo_carregado;
ALTER TABLE rfb_cnpj.arquivo_carregado_new RENAME TO arquivo_carregado;

-- Create index on sha256
CREATE INDEX idx_rfb_cnpj_arquivo_sha256 ON rfb_cnpj.arquivo_carregado (sha256);

COMMIT;

-- ============================================================
-- SCHEMA: pdet
-- Add particao default '', add sha256, add metadados, recreate PK
-- ============================================================
BEGIN;

CREATE TABLE pdet.arquivo_carregado_new (
    nome_arquivo TEXT NOT NULL,
    dataset TEXT NOT NULL,
    particao TEXT NOT NULL DEFAULT '',
    sha256 TEXT,
    linhas BIGINT NOT NULL DEFAULT 0,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    metadados JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (dataset, particao, nome_arquivo)
);

INSERT INTO pdet.arquivo_carregado_new (nome_arquivo, dataset, linhas, carregado_em)
SELECT nome_arquivo, dataset, COALESCE(linhas, 0), carregado_em
FROM pdet.arquivo_carregado;

DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT count(*) INTO old_count FROM pdet.arquivo_carregado;
    SELECT count(*) INTO new_count FROM pdet.arquivo_carregado_new;
    IF old_count != new_count THEN
        RAISE EXCEPTION 'Row count mismatch for pdet: old=%, new=%', old_count, new_count;
    END IF;
    RAISE NOTICE 'pdet: migrated % rows', new_count;
END $$;

DROP TABLE pdet.arquivo_carregado;
ALTER TABLE pdet.arquivo_carregado_new RENAME TO arquivo_carregado;

CREATE INDEX idx_pdet_arquivo_sha256 ON pdet.arquivo_carregado (sha256);

COMMIT;

-- ============================================================
-- SCHEMA: cvm
-- Rename 'arquivo'->'nome_arquivo', add dataset default 'cvm',
-- add particao from 'grupo', add sha256, add metadados, recreate PK
-- ============================================================
BEGIN;

CREATE TABLE cvm.arquivo_carregado_new (
    nome_arquivo TEXT NOT NULL,
    dataset TEXT NOT NULL DEFAULT 'cvm',
    particao TEXT NOT NULL DEFAULT '',
    sha256 TEXT,
    linhas BIGINT NOT NULL DEFAULT 0,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    metadados JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (dataset, particao, nome_arquivo)
);

INSERT INTO cvm.arquivo_carregado_new (nome_arquivo, dataset, particao, linhas, carregado_em)
SELECT arquivo, 'cvm', COALESCE(grupo, ''), COALESCE(linhas, 0), carregado_em
FROM cvm.arquivo_carregado;

DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT count(*) INTO old_count FROM cvm.arquivo_carregado;
    SELECT count(*) INTO new_count FROM cvm.arquivo_carregado_new;
    IF old_count != new_count THEN
        RAISE EXCEPTION 'Row count mismatch for cvm: old=%, new=%', old_count, new_count;
    END IF;
    RAISE NOTICE 'cvm: migrated % rows', new_count;
END $$;

DROP TABLE cvm.arquivo_carregado;
ALTER TABLE cvm.arquivo_carregado_new RENAME TO arquivo_carregado;

CREATE INDEX idx_cvm_arquivo_sha256 ON cvm.arquivo_carregado (sha256);

COMMIT;

-- ============================================================
-- SCHEMA: ibge_sidra
-- Rename 'arquivo'->'nome_arquivo', add dataset default 'ibge_sidra',
-- add particao from 'tabela_sidra_id', add linhas default 0, add sha256,
-- add metadados, recreate PK
-- ============================================================
BEGIN;

CREATE TABLE ibge_sidra.arquivo_carregado_new (
    nome_arquivo TEXT NOT NULL,
    dataset TEXT NOT NULL DEFAULT 'ibge_sidra',
    particao TEXT NOT NULL DEFAULT '',
    sha256 TEXT,
    linhas BIGINT NOT NULL DEFAULT 0,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    metadados JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (dataset, particao, nome_arquivo)
);

INSERT INTO ibge_sidra.arquivo_carregado_new (nome_arquivo, dataset, particao, linhas, carregado_em)
SELECT arquivo, 'ibge_sidra', tabela_sidra_id, 0, carregado_em
FROM ibge_sidra.arquivo_carregado;

DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT count(*) INTO old_count FROM ibge_sidra.arquivo_carregado;
    SELECT count(*) INTO new_count FROM ibge_sidra.arquivo_carregado_new;
    IF old_count != new_count THEN
        RAISE EXCEPTION 'Row count mismatch for ibge_sidra: old=%, new=%', old_count, new_count;
    END IF;
    RAISE NOTICE 'ibge_sidra: migrated % rows', new_count;
END $$;

DROP TABLE ibge_sidra.arquivo_carregado;
ALTER TABLE ibge_sidra.arquivo_carregado_new RENAME TO arquivo_carregado;

CREATE INDEX idx_ibge_sidra_arquivo_sha256 ON ibge_sidra.arquivo_carregado (sha256);

COMMIT;

-- ============================================================
-- SCHEMA: anp
-- Add particao default '', add sha256, add metadados, recreate PK
-- ============================================================
BEGIN;

CREATE TABLE anp.arquivo_carregado_new (
    nome_arquivo TEXT NOT NULL,
    dataset TEXT NOT NULL,
    particao TEXT NOT NULL DEFAULT '',
    sha256 TEXT,
    linhas BIGINT NOT NULL DEFAULT 0,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    metadados JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (dataset, particao, nome_arquivo)
);

INSERT INTO anp.arquivo_carregado_new (nome_arquivo, dataset, linhas, carregado_em)
SELECT nome_arquivo, dataset, COALESCE(linhas, 0), carregado_em
FROM anp.arquivo_carregado;

DO $$
DECLARE
    old_count BIGINT;
    new_count BIGINT;
BEGIN
    SELECT count(*) INTO old_count FROM anp.arquivo_carregado;
    SELECT count(*) INTO new_count FROM anp.arquivo_carregado_new;
    IF old_count != new_count THEN
        RAISE EXCEPTION 'Row count mismatch for anp: old=%, new=%', old_count, new_count;
    END IF;
    RAISE NOTICE 'anp: migrated % rows', new_count;
END $$;

DROP TABLE anp.arquivo_carregado;
ALTER TABLE anp.arquivo_carregado_new RENAME TO arquivo_carregado;

CREATE INDEX idx_anp_arquivo_sha256 ON anp.arquivo_carregado (sha256);

COMMIT;

-- ============================================================
-- SCHEMA: inmet
-- CREATE TABLE IF NOT EXISTS with full canonical schema
-- ============================================================
BEGIN;

CREATE TABLE IF NOT EXISTS inmet.arquivo_carregado (
    nome_arquivo TEXT NOT NULL,
    dataset TEXT NOT NULL,
    particao TEXT NOT NULL DEFAULT '',
    sha256 TEXT,
    linhas BIGINT NOT NULL DEFAULT 0,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    metadados JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (dataset, particao, nome_arquivo)
);

CREATE INDEX IF NOT EXISTS idx_inmet_arquivo_sha256 ON inmet.arquivo_carregado (sha256);

COMMIT;

-- ============================================================
-- SCHEMA: bcb_sgs
-- CREATE TABLE IF NOT EXISTS with full canonical schema
-- ============================================================
BEGIN;

CREATE TABLE IF NOT EXISTS bcb_sgs.arquivo_carregado (
    nome_arquivo TEXT NOT NULL,
    dataset TEXT NOT NULL,
    particao TEXT NOT NULL DEFAULT '',
    sha256 TEXT,
    linhas BIGINT NOT NULL DEFAULT 0,
    carregado_em TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    metadados JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (dataset, particao, nome_arquivo)
);

CREATE INDEX IF NOT EXISTS idx_bcb_sgs_arquivo_sha256 ON bcb_sgs.arquivo_carregado (sha256);

COMMIT;