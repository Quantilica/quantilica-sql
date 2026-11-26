# Fixtures do SIDRA (cortes de respostas reais)

Cortes de respostas públicas do IBGE, baixadas em **25/11/2026** da API do SIDRA
(`https://apisidra.ibge.gov.br/values/...`) e dos metadados de agregados
(`https://servicodados.ibge.gov.br/api/v3/agregados/<id>/metadados`, `/periodos` e
`/localidades/<nível>`). Dados do IBGE (SIDRA); o teste não chama a rede.

Os arquivos seguem o layout do bruto do `ibge-sidra-fetcher`: pasta `t-<id>/`, dados
`t-<id>_..._v-all@AAAAMMDD.json` (a data é a de modificação do período no IBGE),
sidecar `.manifest.json` com a URL completa da consulta, e `metadados@AAAAMMDD.json`
(data da coleta). Os sidecars foram gerados pelo próprio fetcher; só o campo `path`
foi trocado pelo caminho relativo à pasta das fixtures.

## O que foi cortado

- **Dados:** cabeçalho íntegro (primeira linha) e algumas dezenas de linhas, na ordem
  do arquivo original, **sem editar nenhum valor**. O corte guarda até 8 linhas de
  cada símbolo (`-`, `..`, `...`, `X`, `Z`) que apareça na resposta inteira e completa
  com linhas numéricas espaçadas. O `sha256` e o `size_bytes` do sidecar são os do
  arquivo cortado; a `url` é a da consulta inteira.
- **Metadados:** estrutura e chaves iguais às do original (`asdict` do agregado), com
  as listas reduzidas ao que aparece nas linhas (localidades e categorias) mais os
  períodos usados e os dois anteriores. As variáveis ficam inteiras.

## Arquivos

| Pasta | Tabela | Consulta | Para que serve |
|---|---|---|---|
| `t-1086/` | 1086, leite (trimestral) | `n1, v, p, c12716, c12529` | 5 dimensões, 2 classificações |
| `t-1612/` | 1612, lavouras temporárias | `n3 (UF), v, p, c81` | símbolos `-`, `..` e `...` |
| `t-1737/` | 1737, IPCA série histórica | `n1, v, p` | 3 dimensões, mensal |
| `t-6381/` | 6381, PNAD Contínua | `n1, v, p` | símbolo `Z`, trimestre móvel |
| `t-5932/` | 5932, contas trimestrais | `n1, v, p, c11255` | 1 classificação, símbolo `-` |
| `t-7060/` | 7060, IPCA por grupos e itens | `n1, v, p, c315` | classificação com muitos itens |
| `t-6579/` | 6579, população estimada | `n6 (município), v, p` | município, anual |
| `ordens-de-url/t-1086/` | 1086 em 4 ordens de URL | `n,p,v,c`; `c,c,n,v,p`; `n1+n3`; `p,n,v,c` | o número de `D<i>` segue a URL, não a tabela |

Cada variante de `ordens-de-url/` é a resposta real da API para aquela ordem de
segmentos (o sufixo `_o-<ordem>` no nome não vem do fetcher). A de `n1+n3` traz o
nome de território "Brasil e Unidade da Federação" e o símbolo `X`.
