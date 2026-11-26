"""Carga de um arquivo do SIDRA no esquema dimensional, com versões e vigência.

Cada arquivo de dados é uma **versão** (``versao``) e uma transação: dimensões
resolvidas (``INSERT ... ON CONFLICT DO NOTHING`` em lote, com cache entre os
arquivos de uma mesma execução), observações por ``COPY`` para uma tabela
temporária, vigência por célula e ledger juntos.

Vigência (um único princípio). A **afirmação vigente** de uma consulta é a sua
**última versão** (a de maior ``vigencia_em``: a ``modificacao`` com a hora do carimbo
quando houver; sem data é a mais antiga). Uma célula (série, localidade, período)
vale se alguma consulta a afirma na sua versão vigente; entre consultas vence a de
maior ``vigencia_em`` e, no empate, a primeira carregada (``versao.id`` menor). Só a
observação vencedora de cada célula é ``ativo``. Logo:

* uma revisão do IBGE (mesma consulta, data nova) substitui a afirmação da anterior,
  inclusive as células que deixou de trazer: elas saem da vigência, a menos que outra
  consulta as afirme (aí valem por ela);
* o estado ativo não depende da ordem de carga nem de ``--all`` × ``--historico``;
* carregar uma versão mais antiga que outra da mesma consulta só acrescenta história.

``ativo`` é sempre o resultado de ``dados_em(infinito)`` (para versões com data).

Substituir uma versão (mesmo nome, outro ``sha256``, ou ``--force``) apaga as
observações dela e refaz a vigência das células que ela cobria ou cobre. Uma versão
sem células, ou com queda de mais de 50% das células da anterior da mesma consulta, é
recusada sem ``--force``.
"""

from __future__ import annotations

import datetime as dt
import logging
import re
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg
import psycopg.errors
from psycopg import sql
from psycopg.types.json import Jsonb

from quantilica_sql.core.tracker import record_loaded_file
from quantilica_sql.sidra.erros import SidraFonteError
from quantilica_sql.sidra.leitura import ArquivoSidra
from quantilica_sql.sidra.metadados import Metadados

logger = logging.getLogger("quantilica_sql.sidra.carga")

DATASET = "sidra"
_STAGE = "_qsql_sidra_obs"
_ORFAS = "_qsql_sidra_orfas"
_AFET = "_qsql_sidra_afet"
_CAND = "_qsql_sidra_cand"
_VENC = "_qsql_sidra_venc"

QUEDA_MAXIMA = 0.5  # fração das células da versão anterior que uma nova pode perder
_RE_CONSULTA = re.compile(r"^(?P<c>.+?)@\d{8}(?:T\d{6})?(?:\.[A-Za-z0-9]+)?$")
_FREQ_DESCONHECIDA = "nao_reconhecida"


def consulta_do_nome(nome: str) -> str:
    """Nome do arquivo sem o carimbo ``@AAAAMMDD`` e a extensão (a consulta)."""
    m = _RE_CONSULTA.match(nome)
    return m.group("c") if m else nome.rsplit(".", 1)[0]


# chave de ordem das versões: a ``modificacao`` com a hora do carimbo; sem data é a
# mais antiga
_CHAVE = "COALESCE(vigencia_em, '-infinity'::timestamp)"
# última versão de cada consulta da tabela que toca os períodos dados
_SQL_ULTIMAS = (
    "SELECT id FROM (SELECT DISTINCT ON (consulta) id, periodo_ids "
    "FROM {s}.versao WHERE tabela_id = %s "
    f"ORDER BY consulta, {_CHAVE} DESC, id DESC) u WHERE periodo_ids && %s::int[]"
)
# observações dessas versões nas células afetadas; entra por ``observacao_versao_idx``
_SQL_CANDIDATAS = (
    f"CREATE TEMP TABLE {_CAND} AS SELECT o.serie_id, o.localidade_id, "
    "o.periodo_id, o.versao_id, o.valor, o.simbolo, "
    "COALESCE(v.vigencia_em, '-infinity'::timestamp) AS chave "
    "FROM {s}.observacao o JOIN {s}.versao v ON v.id = o.versao_id "
    "JOIN (SELECT DISTINCT serie_id, localidade_id, periodo_id "
    f"FROM pg_temp.{_AFET}) a "
    "ON a.serie_id = o.serie_id AND a.localidade_id = o.localidade_id "
    "AND a.periodo_id = o.periodo_id "
    "WHERE o.versao_id = ANY(%s)"
)


class _Memo:
    """Cache que só vale depois do commit do arquivo: rollback o descarta."""

    def __init__(self) -> None:
        self.ok: dict[Any, int] = {}
        self.pend: dict[Any, int] = {}

    def get(self, chave: Any) -> int | None:
        v = self.pend.get(chave)
        return v if v is not None else self.ok.get(chave)

    def set(self, chave: Any, valor: int) -> None:
        self.pend[chave] = valor

    def confirmar(self) -> None:
        self.ok.update(self.pend)
        self.pend.clear()

    def descartar(self) -> None:
        self.pend.clear()


@dataclass(frozen=True)
class ResultadoArquivo:
    versao_id: int
    linhas: int
    substituiu: bool
    empates_divergentes: int


class Carregador:
    """Carrega arquivos do SIDRA num schema; guarda o cache entre arquivos.

    Args:
        conn: conexão psycopg.
        schema: schema de destino (com o esquema já criado).
        atomic: fábrica do escopo transacional por arquivo (``adapter.atomic_file``),
            chamada com ``replace=False``: a substituição é feita aqui.
    """

    def __init__(self, conn: psycopg.Connection, schema: str, *, atomic: Callable):
        self.conn = conn
        self.schema = schema
        self._s = sql.Identifier(schema)
        self._atomic = atomic
        self._localidades = _Memo()
        self._periodos = _Memo()
        self._series = _Memo()
        self._meta_ok: dict[int, str] = {}
        self._meta_pend: dict[int, str] = {}

    # ---------------------------------------------------------------- API
    def carregar(
        self,
        lido: ArquivoSidra,
        *,
        filename: str,
        sha256: str | None,
        modificacao: dt.date | None,
        vigencia_em: dt.datetime | None = None,
        force: bool = False,
    ) -> ResultadoArquivo:
        """Carrega ``lido`` como a versão ``filename``; tudo ou nada.

        Args:
            modificacao: data de modificação do período na fonte (``None`` = sem data).
            vigencia_em: ``modificacao`` com a hora do carimbo (``@AAAAMMDDTHHMMSS``),
                para ordenar versões do mesmo dia; sem ela, a meia-noite da data.
            force: aceita versão sem células ou com queda de mais de 50% das células
                da anterior da mesma consulta.
        """
        if vigencia_em is None and modificacao is not None:
            vigencia_em = dt.datetime.combine(modificacao, dt.time())
        tid = lido.tabela_id
        try:
            with self._atomic(
                self.conn,
                filename=filename,
                dataset=DATASET,
                particao=str(tid),
                sha256=sha256,
                replace=False,
            ):
                res = self._carregar_na_transacao(
                    lido,
                    filename=filename,
                    sha256=sha256,
                    modificacao=modificacao,
                    vigencia_em=vigencia_em,
                    force=force,
                )
                record_loaded_file(
                    self.conn,
                    schema=self.schema,
                    filename=filename,
                    dataset=DATASET,
                    rows_loaded=res.linhas,
                    particao=str(tid),
                    metadata={
                        "tabela_sidra_id": str(tid),
                        "versao_id": res.versao_id,
                        "modificacao": modificacao.isoformat() if modificacao else None,
                        "substituiu": res.substituiu,
                        "empates_divergentes": res.empates_divergentes,
                    },
                )
        except psycopg.errors.UniqueViolation as exc:
            self._descartar()
            if exc.diag.constraint_name != "observacao_ativa_uq":
                raise
            raise SidraFonteError(
                f"{filename}: célula repetida no arquivo (mesma série, localidade e "
                f"período mais de uma vez): {exc}"
            ) from exc
        except BaseException:
            self._descartar()
            raise
        self._confirmar()
        return res

    # --------------------------------------------------------- transação
    def _confirmar(self) -> None:
        for memo in (self._localidades, self._periodos, self._series):
            memo.confirmar()
        self._meta_ok.update(self._meta_pend)
        self._meta_pend.clear()

    def _descartar(self) -> None:
        for memo in (self._localidades, self._periodos, self._series):
            memo.descartar()
        self._meta_pend.clear()

    def _exec(self, query: str | sql.Composable, params: Any = None) -> psycopg.Cursor:
        if isinstance(query, str):
            query = sql.SQL(query).format(s=self._s)
        return self.conn.execute(query, params)

    def _carregar_na_transacao(
        self,
        lido: ArquivoSidra,
        *,
        filename: str,
        sha256: str | None,
        modificacao: dt.date | None,
        vigencia_em: dt.datetime | None,
        force: bool,
    ) -> ResultadoArquivo:
        tid = lido.tabela_id
        meta = lido.metadados
        consulta = consulta_do_nome(filename)
        n = len(lido.celulas)
        self._exec(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            (zlib.crc32(self.schema.encode()) & 0x7FFFFFFF, tid),
        )
        self._conferir_tamanho(filename, tid, consulta, vigencia_em, n, force)
        self._garantir_tabela(meta)
        self._garantir_dimensoes(lido)
        serie_id, loc_id, per_id = self._resolver(lido)
        periodo_ids = sorted(per_id.values())

        for tmp in (_STAGE, _ORFAS, _AFET, _CAND, _VENC):
            self._exec(f"DROP TABLE IF EXISTS pg_temp.{tmp}")
        self._exec(
            f"CREATE TEMP TABLE {_STAGE} (serie_id integer NOT NULL, "
            "localidade_id integer NOT NULL, periodo_id integer NOT NULL, "
            "valor numeric, simbolo text)"
        )
        with self.conn.cursor() as cur:
            with cur.copy(
                f"COPY {_STAGE} (serie_id, localidade_id, periodo_id, valor, simbolo) "
                "FROM STDIN"
            ) as copy:
                write_row = copy.write_row
                for c in lido.celulas:
                    write_row(
                        (
                            serie_id[c.serie],
                            loc_id[c.localidade],
                            per_id[c.periodo],
                            c.valor,
                            c.simbolo,
                        )
                    )

        vid, antiga = self._abrir_versao(
            tid, consulta, modificacao, vigencia_em, filename, sha256, n, periodo_ids
        )
        empates = self._vigencia(tid, vid, consulta, periodo_ids, antiga)
        for tmp in (_STAGE, _ORFAS, _AFET, _CAND, _VENC):
            self._exec(f"DROP TABLE IF EXISTS pg_temp.{tmp}")
        if empates:
            logger.warning(
                "SIDRA %s: %d célula(s) afirmada(s) por outra consulta com a mesma "
                "data de modificação e valor diferente (vale a primeira carregada).",
                filename,
                empates,
            )
        return ResultadoArquivo(vid, n, antiga is not None, empates)

    def _conferir_tamanho(
        self,
        filename: str,
        tid: int,
        consulta: str,
        vigencia_em: dt.datetime | None,
        n: int,
        force: bool,
    ) -> None:
        """Recusa versão vazia ou que perde mais de 50% das células da anterior da
        mesma consulta (arquivo truncado vira uma "revisão" que apagaria a vigência)."""
        if force:
            return
        if n == 0:
            raise SidraFonteError(
                f"{filename}: o arquivo não tem nenhuma célula de dados (resposta "
                "vazia ou truncada?); uma versão vazia tiraria a consulta inteira da "
                "vigência. Use --force se a fonte realmente esvaziou a consulta."
            )
        anterior = self._exec(
            "SELECT arquivo, linhas FROM {s}.versao WHERE tabela_id = %(t)s "
            "AND consulta = %(q)s AND arquivo <> %(a)s "
            "AND COALESCE(vigencia_em, '-infinity'::timestamp) "
            "< COALESCE(%(v)s::timestamp, '-infinity'::timestamp) "
            "ORDER BY COALESCE(vigencia_em, '-infinity'::timestamp) DESC, id DESC "
            "LIMIT 1",
            {"t": tid, "q": consulta, "a": filename, "v": vigencia_em},
        ).fetchone()
        if anterior and anterior[1] and n < anterior[1] * (1 - QUEDA_MAXIMA):
            raise SidraFonteError(
                f"{filename}: tem {n} célula(s) e a versão anterior da mesma consulta "
                f"({anterior[0]}) tinha {anterior[1]}: queda de mais de "
                f"{QUEDA_MAXIMA:.0%} (arquivo truncado?). Use --force se a revisão da "
                "fonte realmente reduziu a consulta."
            )

    # ----------------------------------------------------------- dimensões
    def _garantir_tabela(self, meta: Metadados) -> None:
        origem = meta.arquivo.name if meta.arquivo else "metadados"
        if self._meta_pend.get(meta.tabela_id) == origem or (
            self._meta_ok.get(meta.tabela_id) == origem
        ):
            return
        bruto = {k: v for k, v in meta.bruto.items() if k != "localidades"}
        self._exec(
            "INSERT INTO {s}.tabela (id, nome, pesquisa, assunto, periodicidade, url, "
            "metadados, metadados_arquivo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (id) DO UPDATE SET nome = EXCLUDED.nome, "
            "pesquisa = EXCLUDED.pesquisa, assunto = EXCLUDED.assunto, "
            "periodicidade = EXCLUDED.periodicidade, url = EXCLUDED.url, "
            "metadados = EXCLUDED.metadados, "
            "metadados_arquivo = EXCLUDED.metadados_arquivo, "
            "atualizado_em = clock_timestamp()",
            (
                meta.tabela_id,
                meta.nome,
                meta.pesquisa,
                meta.assunto,
                meta.periodicidade,
                meta.url,
                Jsonb(bruto),
                origem,
            ),
        )
        # dimensões completas dos metadados (variáveis, classificações, categorias)
        tid = meta.tabela_id
        self._exec(
            "INSERT INTO {s}.variavel AS t (tabela_id, id, nome) "
            "SELECT %s, * FROM unnest(%s::int[], %s::text[]) "
            "ON CONFLICT (tabela_id, id) DO UPDATE SET nome = EXCLUDED.nome "
            "WHERE t.nome IS DISTINCT FROM EXCLUDED.nome",
            (tid, [v.id for v in meta.variaveis], [v.nome for v in meta.variaveis]),
        )
        self._exec(
            "INSERT INTO {s}.classificacao AS t (tabela_id, id, nome, ordem) "
            "SELECT %s, * FROM unnest(%s::int[], %s::text[], %s::smallint[]) "
            "ON CONFLICT (tabela_id, id) DO UPDATE SET nome = EXCLUDED.nome "
            "WHERE t.nome IS DISTINCT FROM EXCLUDED.nome",
            (
                tid,
                [c.id for c in meta.classificacoes],
                [c.nome for c in meta.classificacoes],
                [c.ordem for c in meta.classificacoes],
            ),
        )
        for c in meta.classificacoes:
            if not c.categorias:
                continue
            self._exec(
                "INSERT INTO {s}.categoria AS t (tabela_id, classificacao_id, id, "
                "nome, nivel, unidade) SELECT %s, %s, * FROM unnest(%s::int[], "
                "%s::text[], "
                "%s::smallint[], %s::text[]) "
                "ON CONFLICT (tabela_id, classificacao_id, id) DO UPDATE SET "
                "nome = EXCLUDED.nome, nivel = EXCLUDED.nivel, "
                "unidade = EXCLUDED.unidade WHERE (t.nome, t.nivel, t.unidade) "
                "IS DISTINCT FROM (EXCLUDED.nome, EXCLUDED.nivel, EXCLUDED.unidade)",
                (
                    tid,
                    c.id,
                    [x.id for x in c.categorias],
                    [x.nome for x in c.categorias],
                    [x.nivel for x in c.categorias],
                    [x.unidade for x in c.categorias],
                ),
            )
        self._meta_pend[meta.tabela_id] = origem

    @staticmethod
    def _ordem_da_classificacao(meta: Metadados, cid: int) -> int:
        """Posição da classificação entre as da tabela (a dos metadados), que fixa
        a coluna ``d4``, ``d5``... da view ``dimensao`` qualquer que seja a consulta."""
        c = meta.classificacao(cid)
        if c is None:
            raise SidraFonteError(f"a classificação {cid} não consta nos metadados")
        return c.ordem

    def _garantir_dimensoes(self, lido: ArquivoSidra) -> None:
        """Variáveis e categorias vistas nos dados que os metadados não listam."""
        tid = lido.tabela_id
        variaveis: dict[int, str] = {}
        categorias: dict[tuple[int, int], str] = {}
        for serie in lido.series:
            variaveis[serie.variavel_id] = serie.variavel_nome
            for cid, catid, nome in serie.categorias:
                categorias[(cid, catid)] = nome
        if variaveis:
            self._exec(
                "INSERT INTO {s}.variavel (tabela_id, id, nome) "
                "SELECT %s, * FROM unnest(%s::int[], %s::text[]) "
                "ON CONFLICT DO NOTHING",
                (tid, list(variaveis), list(variaveis.values())),
            )
        if categorias:
            self._exec(
                "INSERT INTO {s}.categoria (tabela_id, classificacao_id, id, nome) "
                "SELECT %s, * FROM unnest(%s::int[], %s::int[], %s::text[]) "
                "ON CONFLICT DO NOTHING",
                (
                    tid,
                    [k[0] for k in categorias],
                    [k[1] for k in categorias],
                    list(categorias.values()),
                ),
            )

    def _resolver(
        self, lido: ArquivoSidra
    ) -> tuple[dict[Any, int], dict[Any, int], dict[str, int]]:
        """Resolve (criando o que falta) os ids de série, localidade e período
        distintos do arquivo."""
        tid = lido.tabela_id
        meta = lido.metadados
        arq = lido.caminho.name

        # localidades
        faltam = {
            loc
            for loc in lido.localidades
            if self._localidades.get((loc.nivel_codigo, loc.codigo)) is None
        }
        if faltam:
            ordem = sorted(faltam)
            self._exec(
                "INSERT INTO {s}.localidade (nivel_codigo, nivel_nome, codigo, nome) "
                "SELECT * FROM unnest(%s::text[], %s::text[], %s::text[], %s::text[]) "
                "ON CONFLICT (nivel_codigo, codigo) DO NOTHING",
                (
                    [x.nivel_codigo for x in ordem],
                    [x.nivel_nome for x in ordem],
                    [x.codigo for x in ordem],
                    [x.nome for x in ordem],
                ),
            )
            rows = self._exec(
                "SELECT l.id, l.nivel_codigo, l.codigo FROM {s}.localidade l "
                "JOIN unnest(%s::text[], %s::text[]) AS u(nc, cod) "
                "ON l.nivel_codigo = u.nc AND l.codigo = u.cod",
                ([x.nivel_codigo for x in ordem], [x.codigo for x in ordem]),
            ).fetchall()
            for id_, nc, cod in rows:
                self._localidades.set((nc, cod), id_)

        # períodos
        codigos = lido.periodos
        for cod in sorted(codigos):
            p = meta.periodos.get(cod)
            if p is None:
                raise SidraFonteError(
                    f"{arq}: o período {cod!r} do arquivo não consta nos "
                    f"{meta.arquivo.name if meta.arquivo else 'metadados'}; coletar "
                    "os metadados de novo."
                )
            freq = p.frequencia or _FREQ_DESCONHECIDA
            if self._periodos.get((freq, cod)) is not None:
                continue
            self._exec(
                "INSERT INTO {s}.periodo (frequencia, codigo, rotulo, ano, mes, "
                "trimestre, semestre, data_inicio, data_fim) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (frequencia, codigo) DO NOTHING",
                (
                    freq,
                    cod,
                    p.rotulo,
                    p.ano,
                    p.mes,
                    p.trimestre,
                    p.semestre,
                    p.data_inicio,
                    p.data_fim,
                ),
            )
            row = self._exec(
                "SELECT id FROM {s}.periodo WHERE frequencia = %s AND codigo = %s",
                (freq, cod),
            ).fetchone()
            self._periodos.set((freq, cod), row[0])
        per_id = {
            cod: self._periodos.get(
                (meta.periodos[cod].frequencia or _FREQ_DESCONHECIDA, cod)
            )
            for cod in codigos
        }

        # séries
        distintas = lido.series
        chaves = {s: _chave_da_serie(s) for s in distintas}
        novas = [s for s in distintas if self._series.get((tid, chaves[s])) is None]
        if novas:
            novas.sort(key=lambda s: chaves[s])
            rows = self._exec(
                "INSERT INTO {s}.serie (tabela_id, variavel_id, unidade_codigo, "
                "unidade_nome, chave) SELECT %s, * FROM unnest(%s::int[], %s::text[], "
                "%s::text[], %s::text[]) ON CONFLICT (tabela_id, chave) DO NOTHING "
                "RETURNING id, chave",
                (
                    tid,
                    [s.variavel_id for s in novas],
                    [s.unidade_codigo for s in novas],
                    [s.unidade_nome for s in novas],
                    [chaves[s] for s in novas],
                ),
            ).fetchall()
            criadas = dict((chave, id_) for id_, chave in rows)
            if criadas:
                ponte = [
                    (criadas[chaves[s]], ordem, cid, catid)
                    for s in novas
                    if chaves[s] in criadas
                    for cid, catid, _n in s.categorias
                    for ordem in [self._ordem_da_classificacao(meta, cid)]
                ]
                if ponte:
                    self._exec(
                        "INSERT INTO {s}.serie_categoria (serie_id, ordem, tabela_id, "
                        "classificacao_id, categoria_id) SELECT u.a, u.b, %s, u.c, u.d "
                        "FROM unnest(%s::int[], %s::smallint[], %s::int[], %s::int[]) "
                        "AS u(a, b, c, d)",
                        (
                            tid,
                            [r[0] for r in ponte],
                            [r[1] for r in ponte],
                            [r[2] for r in ponte],
                            [r[3] for r in ponte],
                        ),
                    )
            for id_, chave in self._exec(
                "SELECT id, chave FROM {s}.serie WHERE tabela_id = %s "
                "AND chave = ANY(%s)",
                (tid, [chaves[s] for s in novas]),
            ):
                self._series.set((tid, chave), id_)
        serie_id = {s: self._series.get((tid, chaves[s])) for s in distintas}
        loc_id = {
            loc: self._localidades.get((loc.nivel_codigo, loc.codigo))
            for loc in lido.localidades
        }
        return serie_id, loc_id, per_id

    # ------------------------------------------------------------- versão
    def _abrir_versao(
        self,
        tid: int,
        consulta: str,
        modificacao: dt.date | None,
        vigencia_em: dt.datetime | None,
        filename: str,
        sha256: str | None,
        linhas: int,
        periodo_ids: list[int],
    ) -> tuple[int, list[int] | None]:
        """Cria a ``versao`` ou, se o arquivo já tinha uma, apaga as observações dela
        (guardando as células na tabela ``_ORFAS``) e a atualiza.

        Devolve ``(id, períodos_da_versão_antiga)``; o segundo é ``None`` se nova.
        """
        row = self._exec(
            "SELECT id, periodo_ids FROM {s}.versao WHERE tabela_id = %s "
            "AND arquivo = %s",
            (tid, filename),
        ).fetchone()
        if row is None:
            novo = self._exec(
                "INSERT INTO {s}.versao (tabela_id, consulta, modificacao, "
                "vigencia_em, arquivo, sha256, linhas, periodo_ids) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                (
                    tid,
                    consulta,
                    modificacao,
                    vigencia_em,
                    filename,
                    sha256,
                    linhas,
                    periodo_ids,
                ),
            ).fetchone()
            return novo[0], None
        vid, periodos_antigos = row
        self._exec(
            f"CREATE TEMP TABLE {_ORFAS} (serie_id integer, localidade_id integer, "
            "periodo_id integer)"
        )
        self._exec(
            "WITH d AS (DELETE FROM {s}.observacao WHERE versao_id = %s "
            "RETURNING serie_id, localidade_id, periodo_id) "
            f"INSERT INTO {_ORFAS} SELECT serie_id, localidade_id, periodo_id FROM d",
            (vid,),
        )
        self._exec(
            "UPDATE {s}.versao SET consulta = %s, modificacao = %s, vigencia_em = %s, "
            "sha256 = %s, linhas = %s, periodo_ids = %s, "
            "carregado_em = clock_timestamp() WHERE id = %s",
            (consulta, modificacao, vigencia_em, sha256, linhas, periodo_ids, vid),
        )
        return vid, list(periodos_antigos or [])

    # ----------------------------------------------------------- vigência
    def _vigencia(
        self,
        tid: int,
        vid: int,
        consulta: str,
        periodo_ids: list[int],
        antiga: list[int] | None,
    ) -> int:
        """Insere as observações da versão (``_STAGE``) e refaz a vigência.

        Devolve quantas células empataram em data com outra consulta e valor
        diferente.
        """
        p = {"t": tid, "v": vid, "q": consulta}
        ultima = self._exec(
            "SELECT id FROM {s}.versao WHERE tabela_id = %(t)s AND consulta = %(q)s "
            f"ORDER BY {_CHAVE} DESC, id DESC LIMIT 1",
            p,
        ).fetchone()[0]
        eh_ultima = ultima == vid
        anterior = self._exec(
            "SELECT id, periodo_ids FROM {s}.versao WHERE tabela_id = %(t)s "
            "AND consulta = %(q)s AND id <> %(v)s "
            f"ORDER BY {_CHAVE} DESC, id DESC LIMIT 1",
            p,
        ).fetchone()
        substituiu = antiga is not None
        afeta_anterior = anterior is not None and (eh_ultima or substituiu)
        inserir = (
            "INSERT INTO {s}.observacao (versao_id, tabela_id, serie_id, "
            "localidade_id, periodo_id, valor, simbolo, ativo) "
            "SELECT %s, %s, serie_id, localidade_id, periodo_id, valor, simbolo, %s "
            f"FROM pg_temp.{_STAGE}"
        )

        if not eh_ultima and not substituiu:
            # versão mais antiga que outra da mesma consulta: só história
            self._exec(inserir, (vid, tid, False))
            return 0

        if eh_ultima and anterior is None and not substituiu:
            # caso comum: período novo. Sem versão da tabela com período em comum não
            # há célula a disputar; senão, sonda as ativas (índice único parcial)
            colide = (
                self._exec(
                    "SELECT EXISTS (SELECT 1 FROM {s}.versao WHERE tabela_id = %s "
                    "AND id <> %s AND periodo_ids && %s::int[])",
                    (tid, vid, periodo_ids),
                ).fetchone()[0]
                and self._exec(
                    f"SELECT EXISTS (SELECT 1 FROM pg_temp.{_STAGE} st "
                    "JOIN {s}.observacao o ON o.serie_id = st.serie_id "
                    "AND o.localidade_id = st.localidade_id "
                    "AND o.periodo_id = st.periodo_id AND o.ativo)"
                ).fetchone()[0]
            )
            if not colide:
                self._exec(inserir, (vid, tid, True))
                return 0

        # caminho geral: insere inativa e refaz a vigência das células afetadas
        self._exec(inserir, (vid, tid, False))
        periodos = set(periodo_ids)
        self._exec(
            f"CREATE TEMP TABLE {_AFET} (serie_id integer, localidade_id integer, "
            "periodo_id integer)"
        )
        if eh_ultima:
            self._exec(
                f"INSERT INTO {_AFET} SELECT serie_id, localidade_id, periodo_id "
                f"FROM pg_temp.{_STAGE}"
            )
        if substituiu:
            periodos.update(antiga)
            self._exec(
                f"INSERT INTO {_AFET} SELECT serie_id, localidade_id, periodo_id "
                f"FROM pg_temp.{_ORFAS}"
            )
        if afeta_anterior:
            periodos.update(anterior[1] or [])
            self._exec(
                f"INSERT INTO {_AFET} SELECT serie_id, localidade_id, periodo_id "
                "FROM {s}.observacao WHERE versao_id = %s",
                (anterior[0],),
            )
        # só as últimas versões de cada consulta, e só as que tocam esses períodos
        ids = [
            r[0] for r in self._exec(_SQL_ULTIMAS, (tid, sorted(periodos))).fetchall()
        ]
        self._exec(_SQL_CANDIDATAS, (ids,))
        self._exec(
            f"CREATE TEMP TABLE {_VENC} AS SELECT DISTINCT ON (serie_id, "
            "localidade_id, periodo_id) serie_id, localidade_id, periodo_id, "
            f"versao_id FROM pg_temp.{_CAND} "
            "ORDER BY serie_id, localidade_id, periodo_id, chave DESC, versao_id ASC"
        )
        empates = self._exec(
            f"SELECT count(*) FROM pg_temp.{_VENC} w "
            f"JOIN pg_temp.{_CAND} cw ON cw.serie_id = w.serie_id "
            "AND cw.localidade_id = w.localidade_id AND cw.periodo_id = w.periodo_id "
            "AND cw.versao_id = w.versao_id "
            f"JOIN pg_temp.{_CAND} cv ON cv.serie_id = w.serie_id "
            "AND cv.localidade_id = w.localidade_id AND cv.periodo_id = w.periodo_id "
            "AND cv.versao_id = %s AND cv.chave = cw.chave "
            "WHERE w.versao_id <> %s AND (cv.valor IS DISTINCT FROM cw.valor "
            "OR cv.simbolo IS DISTINCT FROM cw.simbolo)",
            (vid, vid),
        ).fetchone()[0]
        # primeiro desativa o que perdeu (libera o índice único), depois ativa
        self._exec(
            "UPDATE {s}.observacao o SET ativo = false "
            f"FROM (SELECT DISTINCT serie_id, localidade_id, periodo_id "
            f"FROM pg_temp.{_AFET}) a "
            "WHERE o.ativo AND o.serie_id = a.serie_id "
            "AND o.localidade_id = a.localidade_id AND o.periodo_id = a.periodo_id "
            f"AND NOT EXISTS (SELECT 1 FROM pg_temp.{_VENC} w "
            "WHERE w.serie_id = o.serie_id AND w.localidade_id = o.localidade_id "
            "AND w.periodo_id = o.periodo_id AND w.versao_id = o.versao_id)"
        )
        if ids:
            self._exec(
                f"UPDATE {{s}}.observacao o SET ativo = true FROM pg_temp.{_VENC} w "
                "WHERE o.versao_id = ANY(%s) AND o.versao_id = w.versao_id "
                "AND o.serie_id = w.serie_id AND o.localidade_id = w.localidade_id "
                "AND o.periodo_id = w.periodo_id AND NOT o.ativo",
                (ids,),
            )
        return int(empates)


def _chave_da_serie(serie: Any) -> str:
    """Chave textual da série: variável, unidade e categorias (na ordem dos
    metadados)."""
    cats = ",".join(f"{cid}:{catid}" for cid, catid, _n in serie.categorias)
    return f"{serie.variavel_id}|{serie.unidade_codigo}|{cats}"
