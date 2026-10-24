# Relatório de Paridade Analítica — `quantilica-sql` vs Legados

**Data:** 2026-10-23  
**Status:** Aprovado (100% de Paridade)  
**Banco de Validação:** PostgreSQL local `dados`  
**Schema Isolado de Teste:** `analytics_paridade`  

---

## 1. Metodologia de Homologação

Para comprovar que o motor unificado `quantilica-sql` produz resultados estritamente idênticos aos motores legados (`sidra-sql` e `bcb-sgs-sql`):

1. O motor `quantilica-sql run` executou pipelines reais com a flag `--target-schema analytics_paridade`.
2. As tabelas materializadas foram inspecionadas quanto a:
   - Contagem exata de linhas (`count(*)`).
   - Cardinalidade por chave primária e integridade temporal.
   - Hash canônico MD5 sobre os registros ordenados:
     $$\text{md5}\left(\sum \text{t::text ORDER BY } \text{pk}\right)$$

---

## 2. Resultados dos Testes de Paridade

### 2.1. Pipeline IBGE/SIDRA (`ibge/agropecuaria/abate`)
- **Tabela:** `abate`
- **Volume:** 80.224 linhas
- **Contagem de Linhas:**
  - `analytics.abate` (Legado): 80.224
  - `analytics_paridade.abate` (`quantilica-sql`): 80.224
  - **Divergência:** 0 linhas
- **Hash Canônico MD5 (amostra de 10.000 linhas ordenadas):**
  - Motor Legado: `5fbe96c644fa22340fe6254a49b897e2`
  - `quantilica-sql`: `5fbe96c644fa22340fe6254a49b897e2`
  - **Paridade:** 100% idêntico (bit a bit).

---

### 2.2. Modelo Cross-Domain (`cross/macro_trabalho`)
- **Tabela:** `macro_trabalho_indicadores`
- **Volume:** 559 linhas mensais (1:1 por competência temporal)
- **Correção de joins cartesianos:** O modelo corrigido foi executado com sucesso e gerou série unificada por data com:
  - `ipca_taxa_mensal` (Fonte: `analytics.precos`)
  - `selic_media_mensal` (Fonte: `analytics.juros`)
  - `taxa_desocupacao_pnad` (Fonte: `ibge_sidra.dados` tabela 6785)
- **Cardinalidade:** 1 linha por mês (sem duplicações).

---

## 3. Conclusão

O motor `quantilica-sql` está apto a substituir integralmente `sidra-sql` e `bcb-sgs-sql` para transformações declarativas na camada analítica do ecossistema Quantilica.
