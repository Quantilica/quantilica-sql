"""Adaptador para tabelas agregadas do IBGE SIDRA.

Carrega a resposta crua da API (``/values``) num esquema dimensional (ver
:mod:`quantilica_sql.sidra.esquema`): o papel de cada ``D<i>`` vem da URL do sidecar e
dos metadados (:mod:`quantilica_sql.sidra.papeis`), cada arquivo é uma versão e a
vigência é por célula (:mod:`quantilica_sql.sidra.carga`).
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from quantilica.core.manifests import MANIFEST_SIDECAR_SUFFIX, read_manifest_sidecar

from quantilica_sql.adapters._entrada import arquivos_vigentes
from quantilica_sql.adapters.base import BaseSourceAdapter
from quantilica_sql.core.tracker import ensure_tracker_table
from quantilica_sql.database import ensure_schema
from quantilica_sql.sidra.carga import Carregador
from quantilica_sql.sidra.erros import SidraFonteError
from quantilica_sql.sidra.esquema import esquema_antigo, preparar_esquema
from quantilica_sql.sidra.esquema import resumo as esquema_resumo
from quantilica_sql.sidra.leitura import ler_arquivo
from quantilica_sql.sidra.metadados import metadados_da_pasta

logger = logging.getLogger("quantilica_sql.adapters.sidra")

_RE_TABELA_NO_NOME = re.compile(r"^t-(\d+)(?:_|@|\.|$)")
_RE_CARIMBO = re.compile(r"@(\d{8})(?:T\d{6})?\.json$")
_NAO_SAO_DADOS = ("metadados@", "indice_pesquisas@")
_SEM_DATA = dt.date(1, 1, 1)  # ``@00010101``: o SIDRA não informou a modificação


def _parse_modificacao(value: Any, *, origem: str) -> dt.date | None:
    """Converte uma data de modificação em ``date``; ausente ou ``0001-01-01`` vira
    ``None``. Valor ilegível levanta ``ValueError`` (nunca vira um padrão)."""
    if value is None or value == "":
        return None
    if isinstance(value, dt.datetime):
        parsed: dt.date | None = value.date()
    elif isinstance(value, dt.date):
        parsed = value
    else:
        texto = str(value).strip()
        parsed = None
        try:
            if re.fullmatch(r"\d{8}", texto):
                parsed = dt.datetime.strptime(texto, "%Y%m%d").date()
            elif re.fullmatch(r"\d{2}/\d{2}/\d{4}", texto):
                parsed = dt.datetime.strptime(texto, "%d/%m/%Y").date()
            else:
                parsed = dt.date.fromisoformat(texto[:10])
        except ValueError:
            parsed = None
        if parsed is None:
            raise ValueError(
                f"data de modificacao inválida em {origem}: {value!r} "
                "(esperado AAAA-MM-DD ou AAAAMMDD)"
            )
    return None if parsed == _SEM_DATA else parsed


def modificacao_do_arquivo(path: Path) -> dt.date | None:
    """Data de modificação (do período, na fonte) de um arquivo bruto do SIDRA.

    Ordem: ``metadata.modificacao`` do sidecar ``.manifest.json`` (escrita pelo
    fetcher a partir da API) e, na falta dele, o carimbo ``@AAAAMMDD`` do nome (a
    mesma data). ``0001-01-01``/``@00010101`` (a API não informou) e a ausência de
    ambos dão ``None``. A data de coleta (``fetched_at``, ``metadados@...``) não
    entra: não é a data da fonte.
    """
    sidecar = path.with_name(path.name + MANIFEST_SIDECAR_SUFFIX)
    if sidecar.is_file():
        try:
            manifesto = json.loads(sidecar.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            manifesto = None
        meta = manifesto.get("metadata") if isinstance(manifesto, dict) else None
        if isinstance(meta, dict):
            data = _parse_modificacao(meta.get("modificacao"), origem=sidecar.name)
            if data is not None:
                return data
    m = _RE_CARIMBO.search(path.name)
    return _parse_modificacao(m.group(1), origem=path.name) if m else None


def _id_da_tabela(path: Path) -> str | None:
    """Id da tabela pelo layout do bruto (``t-<id>_...`` ou pasta ``t-<id>``)."""
    for nome in (path.name, path.parent.name):
        m = _RE_TABELA_NO_NOME.match(nome)
        if m:
            return m.group(1)
    return None


def _tabela_do_arquivo(path: Path) -> str:
    """Id da tabela de um arquivo: o nome (``t-<id>_...`` ou pasta ``t-<id>``), o
    ``dataset_id`` do sidecar ou os metadados da pasta. Sem nenhum, erro."""
    id_ = _id_da_tabela(path)
    if id_ is not None:
        return id_
    manifesto = read_manifest_sidecar(path)
    if manifesto is not None:
        m = _RE_TABELA_NO_NOME.match(manifesto.dataset_id or "")
        if m:
            return m.group(1)
    meta = metadados_da_pasta(path.parent)
    if meta is not None:
        return str(meta.tabela_id)
    raise SidraFonteError(
        f"{path}: não deu para saber a tabela do arquivo (esperado "
        "t-<id>_...@AAAAMMDD.json ou um sidecar/metadados que a nomeie)."
    )


class SidraAdapter(BaseSourceAdapter):
    """Adaptador de carga para dados do IBGE SIDRA."""

    @property
    def domain(self) -> str:
        return "sidra"

    @property
    def schema(self) -> str:
        return "ibge_sidra"

    def status(self, conn: psycopg.Connection) -> dict[str, Any]:
        """Tabelas carregadas, versões, observações e ativas (esquema dimensional)."""
        ensure_schema(conn, self.schema)
        ensure_tracker_table(conn, self.schema)
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT count(*), coalesce(sum(linhas), 0) FROM {} "
                    "WHERE dataset = %s"
                ).format(sql.Identifier(self.schema, "arquivo_carregado")),
                (self.domain,),
            )
            arquivos, linhas = cur.fetchone() or (0, 0)
        res = esquema_resumo(conn, self.schema)
        return {
            "domain": self.domain,
            "schema": self.schema,
            "tabelas_carregadas": res["tabelas"],
            "versoes": res["versoes"],
            "observacoes": res["observacoes"],
            "observacoes_ativas": res["observacoes_ativas"],
            "arquivos_carregados": arquivos,
            "linhas_totais": int(linhas),
            "esquema_antigo": esquema_antigo(conn, self.schema),
        }

    def _arquivos_vigentes(
        self,
        raiz: Path,
        tabela: str | None,
        *,
        all_partitions: bool = True,
        historico: bool = False,
    ) -> list[Path]:
        """Arquivos de dados da pasta: só a versão vigente (``@AAAAMMDD`` mais
        nova) de cada consulta ou, com ``historico``, todas as versões, da mais
        antiga para a mais nova. ``metadados@``, ``indice_pesquisas@``, sidecars e
        ``*.tmp`` não são dados."""
        arquivos = arquivos_vigentes(
            raiz,
            all_partitions=all_partitions,
            table=tabela,
            eh_dado=lambda p: not p.name.startswith(_NAO_SAO_DADOS),
            chave_de=_id_da_tabela,
            normaliza_chave=lambda c: c.removeprefix("t-"),
            fonte="SIDRA",
            esperado="t-<id>/t-<id>_...@AAAAMMDD.json",
            logger=logger,
            todas_as_versoes=historico,
        )
        if historico:
            arquivos.sort(
                key=lambda a: (modificacao_do_arquivo(a) or _SEM_DATA, a.name)
            )
        return arquivos

    def load(
        self,
        conn: psycopg.Connection,
        source_path: Path | None = None,
        **kwargs: Any,
    ) -> int:
        """Carrega um arquivo, a pasta de uma tabela ou a raiz do bruto.

        Args:
            source_path: arquivo de dados, pasta ``t-<id>`` ou raiz do bruto.
            all_partitions: ``--all``; numa pasta, a versão vigente de cada consulta.
            table: ``--table <id>``; só essa tabela.
            historico: ``--historico``; todas as versões de cada consulta, em ordem
                de data (reconstrói a história a partir do bruto).
            force: ``--force``; substitui a versão do mesmo arquivo mesmo com o
                mesmo ``sha256``.
            recriar: ``--recriar``; renomeia o ``dados`` do esquema antigo (0.3.x)
                para ``dados_legado_v0`` e cria o esquema novo.

        Returns:
            Observações inseridas (0 quando todos os arquivos já estavam carregados).
        """
        if source_path is None:
            raise ValueError(
                "informe -s/--source-path: um arquivo de dados, a pasta t-<id> ou a "
                "raiz do bruto do SIDRA."
            )
        caminho = Path(source_path)
        force = bool(kwargs.get("force"))
        if caminho.is_dir():
            tabela = kwargs.get("table")
            arquivos = self._arquivos_vigentes(
                caminho,
                str(tabela) if tabela else None,
                all_partitions=bool(kwargs.get("all_partitions") or kwargs.get("all")),
                historico=bool(kwargs.get("historico")),
            )
        elif caminho.is_file():
            arquivos = [caminho]
        else:
            raise ValueError(f"{caminho} não existe.")
        preparar_esquema(conn, self.schema, recriar=bool(kwargs.get("recriar")))
        carregador = Carregador(conn, self.schema, atomic=self.atomic_file)
        total = 0
        for arquivo in arquivos:
            total += self._carregar_arquivo(conn, carregador, arquivo, force=force)
        return total

    def _carregar_arquivo(
        self,
        conn: psycopg.Connection,
        carregador: Carregador,
        arquivo: Path,
        *,
        force: bool,
    ) -> int:
        tabela = _tabela_do_arquivo(arquivo)
        carregar, digest = self.should_load_file(
            conn, arquivo, particao=tabela, force=force
        )
        if not carregar:
            logger.info(
                "Tabela SIDRA %s (%s) já carregada em %s.arquivo_carregado. Pulando.",
                tabela,
                arquivo.name,
                self.schema,
            )
            return 0
        lido = ler_arquivo(arquivo)
        if str(lido.tabela_id) != tabela:
            raise SidraFonteError(
                f"{arquivo}: o nome diz tabela {tabela}, os metadados dizem "
                f"{lido.tabela_id}."
            )
        res = carregador.carregar(
            lido,
            filename=arquivo.name,
            sha256=digest,
            modificacao=modificacao_do_arquivo(arquivo),
        )
        logger.info(
            "SIDRA %s: %d observação(ões)%s.",
            arquivo.name,
            res.linhas,
            " (versão substituída)" if res.substituiu else "",
        )
        return res.linhas
