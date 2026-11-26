"""Metadados do BCB SGS: ``series_metadata`` criada e preenchida pelo adaptador.

Sem banco: leitura e descoberta dos arquivos (``adapters/bcb_metadados.py``) sobre as
fixtures reais de ``tests/data/bcb`` (cortes do que o ``bcb-sgs-fetcher`` gravou) e
sobre variações montadas a partir delas. Com banco (``QUANTILICA_TEST_DATABASE_URL``,
descartável): carga, ledger, renomeação na fonte, atomicidade, papel sem ``CREATE`` e o
ponta a ponta com as SQL de ``macro/*`` do ``quantilica-pipelines``.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import shutil
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from quantilica_sql.adapters.bcb import BcbAdapter
from quantilica_sql.adapters.bcb_metadados import (
    COLUNAS,
    arquivos_de_metadados,
    eh_arquivo_de_metadados,
    id_do_metadado,
    ler_metadado,
)

FIXTURES = Path(__file__).parent / "data" / "bcb"
META = FIXTURES / "bcb-sgs_2026-11" / "metadata"
SERIES = [11, 188, 189, 432, 433]
PONTOS_POR_SERIE = 20  # a API devolveu as últimas 20 observações de cada uma


def _basic(serie: int) -> dict:
    """O dicionário ``basic`` cru do arquivo do fetcher (esperado independente)."""
    return json.loads((META / f"{serie:06d}.json").read_text("utf-8"))["basic"]


def _grava_meta(pasta: Path, serie: int, doc: dict, nome: str | None = None) -> Path:
    pasta.mkdir(parents=True, exist_ok=True)
    arq = pasta / (nome or f"{serie:06d}.json")
    arq.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return arq


@pytest.fixture
def bruto(tmp_path) -> Path:
    """Cópia gravável do bruto real (5 séries, dados e metadados)."""
    raiz = tmp_path / "bcb-sgs"
    shutil.copytree(FIXTURES, raiz, ignore=shutil.ignore_patterns("README.md"))
    return raiz


# --------------------------------------------------------------------------
# Leitura (sem banco)
# --------------------------------------------------------------------------
@pytest.mark.parametrize("serie", SERIES)
def test_leitura_bate_campo_a_campo_com_o_json_do_fetcher(serie):
    raw = json.loads((META / f"{serie:06d}.json").read_text("utf-8"))
    basic = raw["basic"]
    meta = ler_metadado(META / f"{serie:06d}.json")
    assert meta is not None
    assert meta.series_id == basic["series_id"] == serie
    for campo in (
        "name",
        "name_abbreviated",
        "name_english",
        "name_english_abbreviated",
        "frequency",
        "unit",
        "source",
        "series_type",
        "precision",
        "manager_owner",
        "special",
        "formula",
        "primitive_series",
        "warning_message",
    ):
        assert getattr(meta, campo) == basic[campo], campo
    assert meta.theme_hierarchy == (basic["theme_hierarchy"] or None)
    for campo in ("start_date", "end_date"):
        esperado = basic[campo]
        assert getattr(meta, campo) == (
            dt.date.fromisoformat(esperado) if esperado else None
        ), campo
    assert meta.last_update == dt.date.fromisoformat(raw["full"]["last_update"])
    assert len(meta.linha()) == len(COLUNAS)


def test_leitura_ipca_real():
    meta = ler_metadado(META / "000433.json")
    assert meta.name == "Índice nacional de preços ao consumidor-amplo (IPCA)"
    assert meta.theme_hierarchy == [
        "Atividade econômica e preços",
        "Preços",
        "IPCA",
        "IPCA e abertura por grupos",
    ]
    assert (meta.frequency, meta.unit, meta.source) == (
        "Mensal",
        "Var. % mensal",
        "IBGE",
    )
    assert meta.start_date == dt.date(1980, 1, 1) and meta.end_date is None
    assert meta.special is False and meta.precision == 2


def test_campo_ausente_ou_vazio_fica_nulo_e_nada_e_inventado(tmp_path):
    arq = _grava_meta(
        tmp_path,
        433,
        {
            "basic": {
                "series_id": 433,
                "name": "  Só o nome  ",
                "unit": "",
                "theme_hierarchy": [],
                "start_date": "sem data",
                "precision": None,
                "special": None,
            },
            "full": None,
        },
    )
    meta = ler_metadado(arq)
    assert meta.name == "Só o nome"
    for campo in COLUNAS:
        if campo not in ("series_id", "name"):
            assert getattr(meta, campo) is None, campo


def test_forma_sem_basic_e_ignorada_e_basic_nao_objeto_e_erro(tmp_path):
    assert ler_metadado(_grava_meta(tmp_path, 1, {"basic": None, "full": None})) is None
    assert ler_metadado(_grava_meta(tmp_path, 2, {"nome": "Taxa"})) is None
    with pytest.raises(ValueError, match="'basic' deveria ser um objeto"):
        ler_metadado(_grava_meta(tmp_path, 3, {"basic": [1]}))


def test_erros_levam_o_caminho_do_arquivo(tmp_path):
    ruim = tmp_path / "000433.json"
    ruim.write_text('{"basic": {', encoding="utf-8")
    with pytest.raises(ValueError, match=r"000433\.json.*ilegíveis"):
        ler_metadado(ruim)
    with pytest.raises(ValueError, match="esperado um objeto"):
        ler_metadado(_grava_meta(tmp_path, 4, [1, 2]))
    with pytest.raises(ValueError, match="o arquivo é da série 5, .* diz 6"):
        ler_metadado(_grava_meta(tmp_path, 5, {"basic": {"series_id": 6}}))
    with pytest.raises(ValueError, match="theme_hierarchy deveria ser lista"):
        ler_metadado(
            _grava_meta(
                tmp_path, 7, {"basic": {"series_id": 7, "theme_hierarchy": "Preços"}}
            )
        )


def test_forma_basic_dividida_e_o_proprio_dicionario_basico(tmp_path):
    arq = _grava_meta(
        tmp_path,
        433,
        {"series_id": 433, "name": "IPCA", "unit": "%"},
        "000433_basic.json",
    )
    meta = ler_metadado(arq)
    assert (meta.series_id, meta.name, meta.unit) == (433, "IPCA", "%")
    assert meta.last_update is None


def test_valores_minimo_e_maximo_preservam_o_texto_decimal(tmp_path):
    arq = _grava_meta(tmp_path, 1, {"basic": {"series_id": 1}})
    arq.write_text(
        '{"basic": {"series_id": 1, "min_value": 0.8440, "max_value": 5.9038}}', "utf-8"
    )
    meta = ler_metadado(arq)
    assert meta.min_value == Decimal("0.8440") and meta.max_value == Decimal("5.9038")


def test_nome_de_arquivo_de_metadados():
    assert id_do_metadado(Path("000433.json")) == 433
    assert id_do_metadado(Path("000433_basic.json")) == 433
    for nome in (
        "433.json",  # dado solto, não metadado
        "series_433@20261120T090000.json",
        "000433.json.manifest.json",
        "000433_basic.html",
        "000433.json.tmp",
    ):
        assert not eh_arquivo_de_metadados(Path(nome)), nome


# --------------------------------------------------------------------------
# Descoberta (sem banco)
# --------------------------------------------------------------------------
def test_descoberta_um_por_serie_em_ordem_de_id(bruto):
    achados = arquivos_de_metadados(bruto)
    assert [id_do_metadado(p) for p in achados] == SERIES


def test_descoberta_o_mes_mais_novo_vence(bruto):
    novo = bruto / "bcb-sgs_2026-12" / "metadata"
    _grava_meta(novo, 433, {"basic": {"series_id": 433, "name": "IPCA de dezembro"}})
    antigo = bruto / "bcb-sgs_2026-01" / "metadata"
    _grava_meta(antigo, 433, {"basic": {"series_id": 433, "name": "IPCA de janeiro"}})
    [achado] = arquivos_de_metadados(bruto, serie=433)
    assert achado.parent.parent.name == "bcb-sgs_2026-12"


def test_descoberta_metadata_direta_vale_mais_que_os_meses_como_no_fetcher(bruto):
    direta = bruto / "metadata"
    _grava_meta(direta, 433, {"basic": {"series_id": 433, "name": "direta"}})
    [achado] = arquivos_de_metadados(bruto, serie="series_433")
    assert achado.parent == direta


def test_descoberta_combinado_vence_o_basic_na_mesma_pasta(bruto):
    pasta = bruto / "bcb-sgs_2026-11" / "metadata"
    _grava_meta(pasta, 433, {"series_id": 433, "name": "b"}, "000433_basic.json")
    [achado] = arquivos_de_metadados(bruto, serie=433)
    assert achado.name == "000433.json"
    (pasta / "000433.json").unlink()
    [achado] = arquivos_de_metadados(bruto, serie=433)
    assert achado.name == "000433_basic.json"


def test_descoberta_ignora_sidecar_html_e_tmp(bruto):
    pasta = bruto / "bcb-sgs_2026-11" / "metadata"
    for nome in (
        "000433.json.manifest.json",
        "000433_basic.html",
        "000433_full.html",
        "000999.json.tmp",
    ):
        (pasta / nome).write_text("x", "utf-8")
    assert [id_do_metadado(p) for p in arquivos_de_metadados(bruto)] == SERIES


@pytest.mark.parametrize("onde", ["data", "mes", "metadata"])
def test_descoberta_a_partir_da_pasta_data_do_mes_ou_da_metadata(bruto, onde):
    raiz = {
        "data": bruto
        / "data",  # o irmão bcb-sgs_*/metadata é procurado na pasta de cima
        "mes": bruto / "bcb-sgs_2026-11",
        "metadata": bruto / "bcb-sgs_2026-11" / "metadata",
    }[onde]
    assert [id_do_metadado(p) for p in arquivos_de_metadados(raiz)] == SERIES


def test_descoberta_serie_inexistente_ou_invalida(bruto):
    assert arquivos_de_metadados(bruto, serie=7) == []
    assert arquivos_de_metadados(bruto, serie="abc") == []


# --------------------------------------------------------------------------
# Com banco
# --------------------------------------------------------------------------
URL = os.environ.get("QUANTILICA_TEST_DATABASE_URL")
pg_only = pytest.mark.skipif(not URL, reason="QUANTILICA_TEST_DATABASE_URL ausente")

CATALOGO = Path(
    os.environ.get("QUANTILICA_PIPELINES_CATALOG")
    or Path(__file__).resolve().parents[2] / "quantilica-pipelines"
)
needs_catalogo = pytest.mark.skipif(
    not (CATALOGO / "manifest.toml").exists(), reason="quantilica-pipelines ausente"
)


@pytest.fixture
def pg():
    psycopg = pytest.importorskip("psycopg")
    from psycopg import sql

    schema = f"qsql_bcbm_{uuid.uuid4().hex[:8]}"

    class _Bcb(BcbAdapter):
        @property
        def schema(self) -> str:
            return schema

    conn = psycopg.connect(URL, autocommit=True)
    yield _Bcb(), conn, schema
    conn.execute(
        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
    )
    conn.close()


def _um(conn, query: str, params=None):
    return conn.execute(query, params).fetchone()


def _meta(conn, schema: str, serie: int) -> dict:
    cur = conn.execute(
        f'SELECT * FROM "{schema}".series_metadata WHERE series_id = %s', (serie,)
    )
    nomes = [c.name for c in cur.description]
    linha = cur.fetchone()
    return dict(zip(nomes, linha, strict=True)) if linha else {}


def _ledger(conn, schema: str, dataset: str) -> list[tuple]:
    return conn.execute(
        f'SELECT particao, nome_arquivo, sha256 FROM "{schema}".arquivo_carregado '
        "WHERE dataset = %s ORDER BY particao",
        (dataset,),
    ).fetchall()


@pg_only
def test_pg_load_all_carrega_dados_e_metadados_num_banco_novo(bruto, pg):
    adapter, conn, schema = pg
    total = adapter.load(conn, bruto, all_partitions=True)
    assert total == len(SERIES) * PONTOS_POR_SERIE + len(SERIES)

    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_data') == (100,)
    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_metadata') == (5,)
    for serie in SERIES:
        raw = _basic(serie)
        linha = _meta(conn, schema, serie)
        for campo in ("name", "name_abbreviated", "frequency", "unit", "source"):
            assert linha[campo] == raw[campo], (serie, campo)
        assert linha["theme_hierarchy"] == raw["theme_hierarchy"]
        assert linha["start_date"] == dt.date.fromisoformat(raw["start_date"])
        assert linha["end_date"] is None and linha["special"] is False
        assert linha["last_update"] is not None


@pg_only
def test_pg_esquema_de_series_metadata_e_o_que_as_sql_leem(bruto, pg):
    """As colunas e tipos que as 13 SQL de macro/* e setorial/* juntam."""
    adapter, conn, schema = pg
    adapter.load(conn, bruto, all_partitions=True)
    tipos = dict(
        conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = 'series_metadata'",
            (schema,),
        ).fetchall()
    )
    assert tipos["series_id"] == "integer"
    for coluna in ("name", "name_abbreviated", "unit", "frequency"):
        assert tipos[coluna] == "text"
    assert tipos["theme_hierarchy"] == "ARRAY"
    assert set(tipos) == set(COLUNAS) | {"loaded_at"}
    # A chave é a mesma da junção: uma linha por série.
    assert _um(
        conn,
        "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = 'series_metadata' AND i.indisprimary",
        (schema,),
    ) == (1,)
    # O filtro por tema (``@>``) usa o índice GIN.
    conn.execute("SET enable_seqscan = off")
    plano = "\n".join(
        r[0]
        for r in conn.execute(
            f'EXPLAIN SELECT 1 FROM "{schema}".series_metadata '
            "WHERE theme_hierarchy @> ARRAY['Preços']"
        ).fetchall()
    )
    assert "series_metadata_theme_hierarchy_idx" in plano


@pg_only
def test_pg_recarga_idempotente_force_recarrega_so_o_ledger_muda(bruto, pg):
    adapter, conn, schema = pg
    adapter.load(conn, bruto, all_partitions=True)
    antes = _ledger(conn, schema, "bcb-metadata")
    assert [p for p, _, _ in antes] == ["11", "188", "189", "432", "433"]
    assert all(sha for _, _, sha in antes)
    carimbo = _meta(conn, schema, 433)["loaded_at"]

    assert adapter.load(conn, bruto, all_partitions=True) == 0
    assert _ledger(conn, schema, "bcb-metadata") == antes
    assert _meta(conn, schema, 433)["loaded_at"] == carimbo

    # --force recarrega os metadados (5) e os dados idênticos não inserem nada novo.
    assert adapter.load(conn, bruto, all_partitions=True, force=True) == 5
    assert _meta(conn, schema, 433)["loaded_at"] > carimbo
    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_metadata') == (5,)
    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_data') == (100,)


@pg_only
def test_pg_renomeacao_na_fonte_atualiza_e_campo_que_sumiu_vira_nulo(bruto, pg):
    adapter, conn, schema = pg
    adapter.load(conn, bruto, all_partitions=True)
    antes = _meta(conn, schema, 433)
    assert antes["unit"] == "Var. % mensal"

    doc = json.loads((META / "000433.json").read_text("utf-8"))
    doc["basic"]["name"] = "IPCA (nome novo do BCB)"
    doc["basic"]["unit"] = None  # a fonte deixou de informar a unidade
    doc["basic"]["theme_hierarchy"].append("Subtema novo")
    _grava_meta(bruto / "bcb-sgs_2026-12" / "metadata", 433, doc)

    assert adapter.load(conn, bruto, all_partitions=True) == 1  # só a 433 mudou
    depois = _meta(conn, schema, 433)
    assert depois["name"] == "IPCA (nome novo do BCB)"
    assert depois["unit"] is None  # nunca herda o valor antigo
    assert depois["theme_hierarchy"][-1] == "Subtema novo"
    assert depois["loaded_at"] > antes["loaded_at"]
    # as outras séries e as observações ficam como estavam
    assert _meta(conn, schema, 189)["name"] == _basic(189)["name"]
    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_metadata') == (5,)
    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_data') == (100,)
    [(_, nome, sha)] = [
        r for r in _ledger(conn, schema, "bcb-metadata") if r[0] == "433"
    ]
    assert nome == "000433.json" and sha


@pg_only
def test_pg_table_carrega_dados_e_metadados_so_da_serie(bruto, pg):
    adapter, conn, schema = pg
    assert adapter.load(conn, bruto, table="433") == PONTOS_POR_SERIE + 1
    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_metadata') == (1,)
    assert _um(conn, f'SELECT DISTINCT series_id FROM "{schema}".series_data') == (433,)


@pg_only
def test_pg_arquivo_de_metadados_sozinho_carrega_so_o_metadado(bruto, pg):
    adapter, conn, schema = pg
    arq = bruto / "bcb-sgs_2026-11" / "metadata" / "000189.json"
    assert adapter.load(conn, arq) == 1
    assert _meta(conn, schema, 189)["name"] == _basic(189)["name"]
    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_data') == (0,)
    assert adapter.load(conn, arq) == 0
    with pytest.raises(FileNotFoundError):
        adapter.load(conn, bruto / "bcb-sgs_2026-11" / "metadata" / "000555.json")


@pg_only
def test_pg_serie_com_dados_e_sem_metadado_nao_e_erro_e_aparece_no_status(
    bruto, pg, caplog
):
    adapter, conn, schema = pg
    sem = bruto / "data" / "series_99@20261120T090000.json"
    sem.write_text(
        json.dumps([{"series_id": 99, "date": "2026-10-01", "value": "1.5"}]), "utf-8"
    )
    with caplog.at_level(logging.WARNING, logger="quantilica_sql.adapters.bcb"):
        total = adapter.load(conn, bruto, all_partitions=True)
    assert total == 100 + 5 + 1  # 5 séries (dados e metadados) e a 99, só com dados
    assert any(
        "sem metadado" in r.message and "99" in r.message for r in caplog.records
    )

    info = adapter.status(conn)
    assert info["series_sem_metadata"] == 1
    assert info["series_sem_metadata_ids"] == [99]
    assert info["series_metadata_rows"] == 5
    assert info["metadados_carregados"] == 5
    assert info["metadados_sem_dados"] == 0
    assert info["series_distintas"] == 6


@pg_only
def test_pg_status_antes_de_carregar_e_so_com_dados(tmp_path, pg):
    adapter, conn, schema = pg
    vazio = adapter.status(conn)
    assert (vazio["series_metadata_rows"], vazio["series_sem_metadata"]) == (0, 0)

    # Banco da 0.4.0: só series_data, sem series_metadata.
    adapter._ensure_tables(conn)
    conn.execute(f'DROP TABLE "{schema}".series_metadata')
    conn.execute(
        f'INSERT INTO "{schema}".series_data (series_id, date, value) '
        "VALUES (433, '2026-10-01', 0.5)"
    )
    info = adapter.status(conn)
    assert (info["series_sem_metadata"], info["series_sem_metadata_ids"]) == (1, [433])


@pg_only
def test_pg_banco_da_040_ganha_series_metadata_na_proxima_carga(bruto, pg):
    adapter, conn, schema = pg
    conn.execute(f'CREATE SCHEMA "{schema}"')
    conn.execute(
        f'CREATE TABLE "{schema}".series_data (series_id integer NOT NULL, '
        "date date NOT NULL, date_end date, value numeric, "
        "ativo boolean NOT NULL DEFAULT true, "
        "loaded_at timestamptz NOT NULL DEFAULT clock_timestamp())"
    )
    conn.execute(
        f'CREATE UNIQUE INDEX uq_bcb_sgs_series_date ON "{schema}".series_data '
        "(series_id, date) WHERE ativo"
    )
    assert adapter.load(conn, bruto, all_partitions=True) == 105
    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_metadata') == (5,)


@pg_only
def test_pg_metadado_sem_dados_e_carregado_e_contado_no_status(bruto, pg):
    adapter, conn, schema = pg
    so_meta = bruto / "bcb-sgs_2026-11"
    assert adapter.load(conn, so_meta, all_partitions=True) == 5
    info = adapter.status(conn)
    assert info["series_metadata_rows"] == 5
    assert info["metadados_sem_dados"] == 5 and info["series_sem_metadata"] == 0


@pg_only
def test_pg_metadado_corrompido_para_com_o_caminho_e_nada_fica_a_meio(bruto, pg):
    adapter, conn, schema = pg
    ruim = bruto / "bcb-sgs_2026-11" / "metadata" / "000189.json"
    ruim.write_text('{"basic": {"series_id": 189, "name": ', encoding="utf-8")
    with pytest.raises(ValueError, match=r"000189\.json.*ilegíveis"):
        adapter.load(conn, bruto, all_partitions=True)
    assert _meta(conn, schema, 189) == {}
    assert "189" not in [p for p, _, _ in _ledger(conn, schema, "bcb-metadata")]
    # o que veio antes da falha foi commitado por arquivo (11 e 188)
    assert [p for p, _, _ in _ledger(conn, schema, "bcb-metadata")] == ["11", "188"]


@pg_only
def test_pg_atomico_falha_depois_do_upsert_desfaz_a_linha(bruto, pg, monkeypatch):
    adapter, conn, schema = pg
    arq = bruto / "bcb-sgs_2026-11" / "metadata" / "000433.json"
    adapter._ensure_tables(conn)

    def falha(*a, **k):
        raise RuntimeError("falha injetada no ledger")

    monkeypatch.setattr("quantilica_sql.adapters.bcb.record_loaded_file", falha)
    with pytest.raises(RuntimeError, match="injetada"):
        adapter.load(conn, arq)
    assert _meta(conn, schema, 433) == {}
    assert _ledger(conn, schema, "bcb-metadata") == []
    monkeypatch.undo()
    assert adapter.load(conn, arq) == 1
    assert _meta(conn, schema, 433)["name"] == _basic(433)["name"]


@pg_only
def test_pg_papel_sem_create_no_schema_carrega(bruto, pg):
    import psycopg
    from psycopg.conninfo import make_conninfo

    adapter, conn, schema = pg
    adapter.load(conn, bruto / "bcb-sgs_2026-11" / "metadata" / "000433.json")
    papel = f"qsql_r_{uuid.uuid4().hex[:8]}"
    conn.execute(f"CREATE ROLE \"{papel}\" LOGIN PASSWORD '{papel}'")
    try:
        conn.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{papel}"')
        conn.execute(
            f'GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA "{schema}" '
            f'TO "{papel}"'
        )
        conn.execute(f'GRANT DELETE ON "{schema}".arquivo_carregado TO "{papel}"')
        assert _um(
            conn, "SELECT has_schema_privilege(%s, %s, 'CREATE')", (papel, schema)
        ) == (False,)
        with psycopg.connect(
            make_conninfo(URL, user=papel, password=papel), autocommit=True
        ) as restrito:
            assert adapter.load(restrito, bruto, all_partitions=True) == 104
    finally:
        conn.execute(f'DROP OWNED BY "{papel}"')
        conn.execute(f'DROP ROLE "{papel}"')
    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_metadata') == (5,)


@pg_only
def test_pg_cli_real_load_bcb_sgs_all_sai_com_zero(bruto, pg, monkeypatch):
    from typer.testing import CliRunner

    from quantilica_sql import cli

    adapter, conn, schema = pg
    monkeypatch.setattr(cli, "get_adapter", lambda domain: adapter)

    class _Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(cli, "get_connection", lambda **kw: _Ctx())
    res = CliRunner().invoke(cli.app, ["load", "bcb-sgs", "-s", str(bruto), "--all"])
    assert res.exit_code == 0, res.output
    assert "105 linhas" in res.output
    assert _um(conn, f'SELECT count(*) FROM "{schema}".series_metadata') == (5,)


# --------------------------------------------------------------------------
# Ponta a ponta: load bcb --all num banco novo e as SQL reais do catálogo
# --------------------------------------------------------------------------
def _dados(serie: int) -> list[dict]:
    arq = next((FIXTURES / "data").glob(f"series_{serie}@*.json"))
    return json.loads(arq.read_text("utf-8"))


@pg_only
@needs_catalogo
def test_ponta_a_ponta_load_bcb_all_e_run_macro_bcb_precos_e_juros(
    bruto, pg, monkeypatch
):
    """Sem nenhuma tabela declarada pelo teste: ``load`` cria ``series_metadata`` e as
    SQL de ``macro/*`` do catálogo rodam pelo runner real."""
    from quantilica_sql import runner as runner_mod

    adapter, conn, schema = pg
    adapter.load(conn, bruto, all_partitions=True)

    destino = f"qsql_an_{uuid.uuid4().hex[:8]}"

    class _Ctx:
        def __enter__(self):
            return conn

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(runner_mod, "get_connection", lambda: _Ctx())
    monkeypatch.setattr(runner_mod, "WORKSPACE_CATALOG", str(CATALOGO))
    try:
        for pipeline in ("macro/bcb_precos", "macro/juros"):
            res = runner_mod.run_pipeline(
                pipeline, raw_schema=schema, target_schema=destino
            )
            assert res["status"] == "success"

        # macro/bcb_precos: 433 (IPCA), 188 (INPC) e 189 (IGP-M) têm o tema 'Preços'
        precos = conn.execute(
            f"SELECT indice, nome_serie, unidade, frequencia, count(*), "
            f'min(date), max(date) FROM "{destino}".precos GROUP BY 1, 2, 3, 4 '
            "ORDER BY 1"
        ).fetchall()
        esperado = []
        for indice, serie in (("IGP-M", 189), ("INPC", 188), ("IPCA", 433)):
            b, pts = _basic(serie), _dados(serie)
            datas = [dt.date.fromisoformat(p["date"]) for p in pts]
            esperado.append(
                (indice, b["name"], b["unit"], b["frequency"], len(pts))
                + (min(datas), max(datas))
            )
        assert precos == esperado

        # macro/juros: só a Selic diária (11) tem o tema 'Taxas de juros'
        juros = conn.execute(
            f"SELECT taxa, nome_serie, unidade, frequencia, valor "
            f'FROM "{destino}".juros ORDER BY date'
        ).fetchall()
        b11 = _basic(11)
        assert len(juros) == len(_dados(11))
        assert {j[:4] for j in juros} == {
            ("SELIC-OVER", b11["name"], b11["unit"], b11["frequency"])
        }
        assert [j[4] for j in juros] == [float(p["value"]) for p in _dados(11)]
    finally:
        conn.execute(f'DROP SCHEMA IF EXISTS "{destino}" CASCADE')
