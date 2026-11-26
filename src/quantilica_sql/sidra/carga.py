"""Carga de um arquivo do SIDRA no esquema dimensional, com versões e vigência.

Cada arquivo de dados é uma **versão** (``versao``) e uma transação: dimensões
resolvidas (``INSERT ... ON CONFLICT DO NOTHING`` em lote, com cache entre os
arquivos de uma mesma execução), observações por ``COPY`` para uma tabela
temporária, vigência por célula e ledger juntos.

Vigência. Só **uma** observação por célula (série, localidade, período) é ``ativo``:
a da versão de maior ``modificacao`` (sem data é a mais antiga; ``@00010101``).

* uma versão **mais nova da mesma consulta** (mesmo nome, outra data: revisão do
  IBGE) desativa **todas** as células ativas da anterior, inclusive as que ela deixou
  de trazer (saem da vigência e ficam como história);
* uma consulta **diferente** que cobre a mesma célula (fronteira de pedaço que mudou)
  disputa pela ``modificacao``: a maior vence; empate mantém a que já estava ativa
  (e avisa, com a contagem, se o valor diferir);
* carregar uma versão **mais antiga** que outra já presente da mesma consulta
  entra só como história (inativa).

Substituir uma versão (mesmo nome, outro ``sha256``, ou ``--force``) apaga as
observações dela e refaz a vigência: a célula que só ela cobria sai da vigência e a
que outra consulta também cobria volta a valer por essa outra.
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
_RE_CONSULTA = re.compile(r"^(?P<c>.+?)@\d{8}(?:T\d{6})?(?:\.[A-Za-z0-9]+)?$")
_SEM_DATA = "'-infinity'::date"
_FREQ_DESCONHECIDA = "nao_reconhecida"


def consulta_do_nome(nome: str) -> str:
    """Nome do arquivo sem o carimbo ``@AAAAMMDD`` e a extensão (a consulta)."""
    m = _RE_CONSULTA.match(nome)
    return m.group("c") if m else nome.rsplit(".", 1)[0]


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
    ) -> ResultadoArquivo:
        """Carrega ``lido`` como a versão ``filename``; tudo ou nada."""
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
                    lido, filename=filename, sha256=sha256, modificacao=modificacao
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
    ) -> ResultadoArquivo:
        tid = lido.tabela_id
        meta = lido.metadados
        self._exec(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            (zlib.crc32(self.schema.encode()) & 0x7FFFFFFF, tid),
        )
        self._garantir_tabela(meta)
        self._garantir_dimensoes(lido)
        serie_id, loc_id, per_id = self._resolver(lido)
        consulta = consulta_do_nome(filename)

        self._exec(f"DROP TABLE IF EXISTS pg_temp.{_STAGE}")
        self._exec(f"DROP TABLE IF EXISTS pg_temp.{_ORFAS}")
        self._exec(
            f"CREATE TEMP TABLE {_STAGE} (serie_id integer NOT NULL, "
            "localidade_id integer NOT NULL, periodo_id integer NOT NULL, "
            "valor numeric, simbolo text, ativo boolean NOT NULL DEFAULT true)"
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

        versao_id, substituiu = self._abrir_versao(
            tid,
            consulta,
            modificacao,
            filename,
            sha256,
            len(lido.celulas),
            sorted(per_id.values()),
        )
        empates = self._vigencia(
            tid, versao_id, consulta, modificacao, sorted(per_id.values())
        )
        self._exec(
            "INSERT INTO {s}.observacao (versao_id, tabela_id, serie_id, "
            "localidade_id, periodo_id, valor, simbolo, ativo) "
            "SELECT %s, %s, serie_id, localidade_id, periodo_id, valor, simbolo, "
            f"ativo FROM pg_temp.{_STAGE}",
            (versao_id, tid),
        )
        if substituiu:
            self._reconciliar(tid, consulta)
        self._exec(f"DROP TABLE IF EXISTS pg_temp.{_STAGE}")
        self._exec(f"DROP TABLE IF EXISTS pg_temp.{_ORFAS}")
        if empates:
            logger.warning(
                "SIDRA %s: %d célula(s) já ativa(s) com a mesma data de modificação "
                "e valor diferente (a anterior foi mantida).",
                filename,
                empates,
            )
        return ResultadoArquivo(versao_id, len(lido.celulas), substituiu, empates)

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
            "INSERT INTO {s}.variavel (tabela_id, id, nome) "
            "SELECT %s, * FROM unnest(%s::int[], %s::text[]) ON CONFLICT DO NOTHING",
            (tid, [v.id for v in meta.variaveis], [v.nome for v in meta.variaveis]),
        )
        self._exec(
            "INSERT INTO {s}.classificacao (tabela_id, id, nome, ordem) "
            "SELECT %s, * FROM unnest(%s::int[], %s::text[], %s::smallint[]) "
            "ON CONFLICT DO NOTHING",
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
                "INSERT INTO {s}.categoria (tabela_id, classificacao_id, id, nome, "
                "nivel, unidade) SELECT %s, %s, * FROM unnest(%s::int[], %s::text[], "
                "%s::smallint[], %s::text[]) ON CONFLICT DO NOTHING",
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
                    for ordem, (cid, catid, _n) in enumerate(s.categorias)
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
        filename: str,
        sha256: str | None,
        linhas: int,
        periodo_ids: list[int],
    ) -> tuple[int, bool]:
        """Cria a ``versao`` ou, se o arquivo já tinha uma, apaga as observações
        dela (guardando as células que ela mantinha ativas) e a atualiza."""
        row = self._exec(
            "SELECT id FROM {s}.versao WHERE tabela_id = %s AND arquivo = %s",
            (tid, filename),
        ).fetchone()
        if row is None:
            novo = self._exec(
                "INSERT INTO {s}.versao (tabela_id, consulta, modificacao, arquivo, "
                "sha256, linhas, periodo_ids) VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "RETURNING id",
                (tid, consulta, modificacao, filename, sha256, linhas, periodo_ids),
            ).fetchone()
            return novo[0], False
        vid = row[0]
        self._exec(
            f"CREATE TEMP TABLE {_ORFAS} (serie_id integer, localidade_id integer, "
            "periodo_id integer)"
        )
        self._exec(
            "WITH d AS (DELETE FROM {s}.observacao WHERE versao_id = %s "
            "RETURNING serie_id, localidade_id, periodo_id, ativo) "
            f"INSERT INTO {_ORFAS} SELECT serie_id, localidade_id, periodo_id "
            "FROM d WHERE ativo",
            (vid,),
        )
        self._exec(
            "UPDATE {s}.versao SET consulta = %s, modificacao = %s, sha256 = %s, "
            "linhas = %s, periodo_ids = %s, carregado_em = clock_timestamp() "
            "WHERE id = %s",
            (consulta, modificacao, sha256, linhas, periodo_ids, vid),
        )
        return vid, True

    # ----------------------------------------------------------- vigência
    def _vigencia(
        self,
        tid: int,
        vid: int,
        consulta: str,
        modificacao: dt.date | None,
        periodo_ids: list[int],
    ) -> int:
        """Marca ``ativo`` na tabela de passagem e desativa as ativas superadas.

        Devolve quantas células empataram em data com valor diferente.
        """
        p = {"t": tid, "v": vid, "q": consulta, "m": modificacao}
        superada = self._exec(
            "SELECT EXISTS (SELECT 1 FROM {s}.versao WHERE tabela_id = %(t)s "
            "AND consulta = %(q)s AND id <> %(v)s "
            f"AND COALESCE(modificacao, {_SEM_DATA}) "
            f"> COALESCE(%(m)s::date, {_SEM_DATA}))",
            p,
        ).fetchone()[0]
        if superada:
            # há versão mais nova da mesma consulta: esta entra só como história
            self._exec(f"UPDATE pg_temp.{_STAGE} SET ativo = false")
            return 0
        # (A) a versão anterior da mesma consulta sai inteira da vigência
        self._exec(
            "UPDATE {s}.observacao o SET ativo = false FROM {s}.versao a "
            "WHERE o.versao_id = a.id AND o.ativo AND a.tabela_id = %(t)s "
            "AND a.consulta = %(q)s AND a.id <> %(v)s "
            f"AND COALESCE(a.modificacao, {_SEM_DATA}) "
            f"< COALESCE(%(m)s::date, {_SEM_DATA})",
            p,
        )
        # outra versão da tabela com período em comum? Sem ela (o caso comum: período
        # novo) não há célula a disputar
        if not self._exec(
            "SELECT EXISTS (SELECT 1 FROM {s}.versao WHERE tabela_id = %s "
            "AND id <> %s AND periodo_ids && %s::int[])",
            (tid, vid, periodo_ids),
        ).fetchone()[0]:
            return 0
        colide = self._exec(
            f"SELECT EXISTS (SELECT 1 FROM pg_temp.{_STAGE} st "
            "JOIN {s}.observacao o ON o.serie_id = st.serie_id "
            "AND o.localidade_id = st.localidade_id "
            "AND o.periodo_id = st.periodo_id AND o.ativo)"
        ).fetchone()[0]
        if not colide:
            return 0
        # (B1) empate de data com valor diferente: só conta (aviso)
        empates = self._exec(
            f"SELECT count(*) FROM pg_temp.{_STAGE} st "
            "JOIN {s}.observacao o ON o.serie_id = st.serie_id "
            "AND o.localidade_id = st.localidade_id "
            "AND o.periodo_id = st.periodo_id AND o.ativo "
            "JOIN {s}.versao a ON a.id = o.versao_id "
            f"WHERE COALESCE(a.modificacao, {_SEM_DATA}) "
            f"= COALESCE(%(m)s::date, {_SEM_DATA}) "
            "AND (o.valor IS DISTINCT FROM st.valor "
            "OR o.simbolo IS DISTINCT FROM st.simbolo)",
            p,
        ).fetchone()[0]
        # (B2) onde há ativa de versão não mais antiga (mais nova ou empate), esta
        # observação entra inativa
        self._exec(
            f"UPDATE pg_temp.{_STAGE} st SET ativo = false WHERE EXISTS ("
            "SELECT 1 FROM {s}.observacao o JOIN {s}.versao a ON a.id = o.versao_id "
            "WHERE o.serie_id = st.serie_id AND o.localidade_id = st.localidade_id "
            "AND o.periodo_id = st.periodo_id AND o.ativo "
            f"AND COALESCE(a.modificacao, {_SEM_DATA}) "
            f">= COALESCE(%(m)s::date, {_SEM_DATA}))",
            p,
        )
        # (B3) as ativas que sobraram nas células em que esta vence são mais antigas
        self._exec(
            "UPDATE {s}.observacao o SET ativo = false "
            f"FROM pg_temp.{_STAGE} st WHERE st.ativo AND o.ativo "
            "AND o.serie_id = st.serie_id AND o.localidade_id = st.localidade_id "
            "AND o.periodo_id = st.periodo_id"
        )
        return int(empates)

    def _reconciliar(self, tid: int, consulta: str) -> None:
        """Após substituir uma versão: a célula que ela mantinha ativa e ficou sem
        ativa volta a valer pela melhor observação de **outra consulta**."""
        self._exec(
            "WITH cand AS (SELECT DISTINCT ON (f.serie_id, f.localidade_id, "
            "f.periodo_id) o.ctid AS c "
            f"FROM pg_temp.{_ORFAS} f "
            "JOIN {s}.observacao o ON o.serie_id = f.serie_id "
            "AND o.localidade_id = f.localidade_id AND o.periodo_id = f.periodo_id "
            "JOIN {s}.versao a ON a.id = o.versao_id "
            "WHERE o.tabela_id = %(t)s AND a.consulta <> %(q)s "
            "AND NOT EXISTS (SELECT 1 FROM {s}.observacao x "
            "WHERE x.serie_id = f.serie_id AND x.localidade_id = f.localidade_id "
            "AND x.periodo_id = f.periodo_id AND x.ativo) "
            "ORDER BY f.serie_id, f.localidade_id, f.periodo_id, "
            "a.modificacao DESC NULLS LAST, a.id DESC) "
            "UPDATE {s}.observacao o SET ativo = true FROM cand WHERE o.ctid = cand.c",
            {"t": tid, "q": consulta},
        )


def _chave_da_serie(serie: Any) -> str:
    """Chave textual da série: variável, unidade e categorias (na ordem dos
    metadados)."""
    cats = ",".join(f"{cid}:{catid}" for cid, catid, _n in serie.categorias)
    return f"{serie.variavel_id}|{serie.unidade_codigo}|{cats}"
