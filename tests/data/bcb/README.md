# Fixtures do BCB SGS (cortes de respostas reais)

Cinco séries públicas do SGS do Banco Central, coletadas em **26/11/2026** pelo
`bcb-sgs-fetcher` (0.15.0) e gravadas por ele no layout do bruto. O teste não chama a
rede.

| Série | Nome | Periodicidade |
|---|---|---|
| 433 | IPCA | Mensal |
| 189 | IGP-M | Mensal |
| 188 | INPC | Mensal |
| 11 | Selic | Diária |
| 432 | Meta Selic definida pelo Copom | Diária |

## Layout

```
data/series_<id>@20261120T090000.json         # observações (fetcher: write_series_data)
bcb-sgs_2026-11/metadata/<id:06d>.json        # {"basic": ..., "full": ...} (write_combined_metadata)
```

- **Dados:** as últimas 20 observações da API de dados do SGS
  (`https://api.bcb.gov.br/dados/serie/bcdata.sgs.<id>/dados/ultimos/20`), na forma que
  o fetcher grava (`series_id`, `date`, `value`, `date_end`). O carimbo do nome
  (`@20261120T090000`) é fixo, para o teste não depender do dia da coleta.
- **Metadados:** a página de metadados do SGS (dados básicos e metadados completos),
  lida e convertida pelo `parse_metadata_basic`/`parse_metadata_full` do fetcher. Nenhum
  valor foi editado; o conjunto inteiro de cada série está aqui (são ~5 KB por série).
- Os sidecars `.manifest.json` do fetcher ficam de fora (levam caminhos da máquina que
  coletou); os testes que precisam de um sidecar o criam.

Fonte dos dados: Banco Central do Brasil (SGS), com os dados de origem do IBGE (IPCA,
INPC) e da FGV (IGP-M), como declara o campo `source` de cada metadado.
