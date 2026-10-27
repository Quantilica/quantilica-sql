"""Marts do Tesouro Direto (catálogo) sobre um lake sintético, com resultado conhecido.

Exige ``QUANTILICA_TEST_DATABASE_URL`` (banco descartável), duckdb e o catálogo
``quantilica-pipelines`` ao lado.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest

pytest.importorskip("duckdb")

from quantilica_sql.transform_runner import TransformRunner  # noqa: E402

URL = os.environ.get("QUANTILICA_TEST_DATABASE_URL")
CATALOG = Path(__file__).resolve().parents[2] / "quantilica-pipelines"
MARTS = CATALOG / "tesouro_direto" / "marts" / "transform.toml"
pytestmark = [
    pytest.mark.skipif(not URL, reason="QUANTILICA_TEST_DATABASE_URL ausente"),
    pytest.mark.skipif(not MARTS.exists(), reason="quantilica-pipelines ausente"),
]

INV_2018 = (
    "Codigo do Investidor;Data de Adesao;Estado Civil;Genero;Profissao;"
    "Idade do Investidor;UF do Investidor;Cidade do Investidor;Pais do Investidor;"
    "Situacao da Conta;Operou 12 Meses\n"
    "1;01/02/2018;Solteiro;M;Engenheiro;25;SP;Sao Paulo;Brasil;A;Sim\n"
    "2;03/04/2018;Casado;F;Medico;41;RJ;Rio;Brasil;A;Nao\n"
    "3;05/06/2018;;F;;;SP;Sao Paulo;Brasil;I;Nao\n"
)
INV_2019 = (
    "Codigo do Investidor;Data de Adesao;Estado Civil;Genero;Profissao;"
    "Idade do Investidor;UF do Investidor;Cidade do Investidor;Pais do Investidor;"
    "Situacao da Conta;Operou 12 Meses\n"
    "1;10/01/2019;Casado;M;Engenheiro;26;SP;Sao Paulo;Brasil;A;Sim\n"
    "4;11/02/2019;Solteiro;M;Medico;70;MG;BH;Brasil;A;Sim\n"
)
OPS_2019 = (
    "Codigo do Investidor;Data da Operacao;Tipo Titulo;Vencimento do Titulo;"
    "Quantidade;Valor do Titulo;Valor da Operacao;Tipo da Operacao;Canal da Operacao\n"
    "1;15/01/2019;Tesouro Selic;01/03/2025;1,5;10000,00;1.500,50;C;Internet\n"
    "2;20/01/2019;Tesouro Selic;01/03/2025;1;10000,00;1.000,00;V;Internet\n"
    "5;21/02/2019;Tesouro IPCA+;15/05/2035;2;3000,00;3.000,00;C;Internet\n"
    "1;22/02/2019;NTN-B1;15/12/2049;1;100,00;100,00;C;Internet\n"
    "6;23/02/2019;NTN-B1;15/12/2050;1;100,00;50,00;C;Internet\n"
    ";24/02/2019;Tesouro Selic;01/03/2025;1;1;1;C;Internet\n"
)


@pytest.fixture
def lake(tmp_path, write_hive) -> Path:
    raiz = tmp_path / "lake"
    for dataset, alias, chave, periodo, texto in (
        (
            "investidores-do-tesouro-direto",
            "investidores",
            "investidores-do-tesouro-direto-de-2018",
            "2018",
            INV_2018,
        ),
        (
            "investidores-do-tesouro-direto",
            "investidores",
            "investidores-do-tesouro-direto-de-2019",
            "2019",
            INV_2019,
        ),
        (
            "operacoes-do-tesouro-direto",
            "operacoes",
            "operacoes-do-tesouro-direto-de-2019",
            "2019",
            OPS_2019,
        ),
    ):
        write_hive(
            raiz,
            "tesouro-direto",
            dataset,
            texto,
            key=chave,
            partition=periodo,
            alias=alias,
        )
    return raiz


@pytest.fixture
def conn():
    with psycopg.connect(URL) as c:
        c.execute("SET TIME ZONE 'UTC'")
        c.execute("DROP SCHEMA IF EXISTS tesouro_direto CASCADE")
        c.execute("CREATE SCHEMA tesouro_direto")
        c.execute("CREATE TABLE tesouro_direto.sale (value numeric(18,2))")
        c.execute("INSERT INTO tesouro_direto.sale VALUES (100.50), (200.00)")
        c.execute("CREATE TABLE tesouro_direto.bond (id smallint)")
        c.execute("INSERT INTO tesouro_direto.bond VALUES (1), (2)")
        c.execute("CREATE TABLE tesouro_direto.bond_price (reference_date date)")
        c.execute(
            "INSERT INTO tesouro_direto.bond_price "
            "VALUES ('2026-09-30'), ('2026-10-01')"
        )
        # uma MV antiga com o mesmo nome de um mart: a troca deve substituí-la
        c.execute(
            "CREATE MATERIALIZED VIEW tesouro_direto.mv_investors_growth AS "
            "SELECT now() AS growth_month, 0::bigint AS new_investors"
        )
        c.commit()
        yield c
        c.rollback()
        c.execute("DROP SCHEMA IF EXISTS tesouro_direto CASCADE")
        c.commit()


def _q(conn, table: str, order: str = "1, 2") -> list[tuple]:
    return conn.execute(
        f"SELECT * FROM tesouro_direto.{table} ORDER BY {order}"
    ).fetchall()


def test_marts_calculam_o_esperado(lake, conn):
    done = TransformRunner(raw_schema="tesouro_direto", lake=lake).run_transform_toml(
        conn, MARTS
    )
    assert len(done) == 16

    assert _q(conn, "mart_microdata_totais", "1") == [(6, 3, 5)]
    assert _q(conn, "mv_dashboard_stats", "1") == [
        (1, 6, 3, 5, 300.50, 2, __import__("datetime").date(2026, 10, 1))
    ]

    demog = {(g, f): n for g, f, n in _q(conn, "mv_investor_demographics", "1, 2")}
    assert demog == {
        ("M", "21-30"): 1,  # investidor 1: a idade de 2019 (26) substitui a de 2018
        ("F", "41-50"): 1,
        ("M", "61-70"): 1,
        ("F", "70+"): 1,  # sem idade cai em 70+ (comportamento herdado da MV)
        (None, "70+"): 2,  # investidores 5 e 6 só existem em operações
    }
    assert dict(_q(conn, "mv_investor_stats_by_state", "1")) == {
        "SP": 2,
        "RJ": 1,
        "MG": 1,
    }
    assert dict(_q(conn, "mv_investors_by_region", "1")) == {"Sudeste": 4}
    assert dict(_q(conn, "mv_investors_by_marital_status", "1")) == {
        "Casado": 2,
        "Solteiro": 1,
    }
    assert dict(_q(conn, "mv_investors_by_profession", "1")) == {
        "Medico": 2,
        "Engenheiro": 1,
    }
    assert _q(conn, "mv_investors_profession_gender", "1, 2") == [
        ("Engenheiro", "M", 1),
        ("Medico", "F", 1),
        ("Medico", "M", 1),
    ]
    assert _q(conn, "mv_investors_state_marital", "1, 2") == [
        ("MG", "Solteiro", 1),
        ("RJ", "Casado", 1),
        ("SP", "Casado", 1),
    ]

    crescimento = _q(conn, "mv_investors_growth", "1")
    assert [m.strftime("%Y-%m") for m, _ in crescimento] == [
        "2018-02",
        "2018-04",
        "2018-06",
        "2019-01",
        "2019-02",
    ]
    assert {n for _, n in crescimento} == {1}

    ops = {
        (m.strftime("%Y-%m"), t, b): (n, float(v))
        for m, t, b, n, v in _q(conn, "mv_operations_monthly", "1, 2, 3")
    }
    assert ops == {
        ("2019-01", "C", "Tesouro Selic"): (1, 1500.50),
        ("2019-01", "V", "Tesouro Selic"): (1, 1000.00),
        ("2019-02", "C", "Tesouro IPCA+"): (1, 3000.00),
        ("2019-02", "C", "Tesouro RendA+"): (1, 100.00),  # NTN-B1, 15/12/2049
        ("2019-02", "C", "Tesouro EducA+"): (1, 50.00),  # NTN-B1, 15/12/2050
    }


def test_marts_substituem_a_materialized_view_e_recriam_os_indices(lake, conn):
    TransformRunner(raw_schema="tesouro_direto", lake=lake).run_transform_toml(
        conn, MARTS
    )
    kind = conn.execute(
        "SELECT relkind FROM pg_class "
        "WHERE oid = 'tesouro_direto.mv_investors_growth'::regclass"
    ).fetchone()[0]
    assert kind == "r"
    indices = {
        r[0]
        for r in conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname='tesouro_direto' "
            "AND tablename='mv_operations_monthly'"
        )
    }
    assert indices == {
        "uq_mv_operations_monthly",
        "idx_mv_operations_monthly_bond_type",
        "idx_mv_operations_monthly_month",
        "idx_mv_operations_monthly_type",
    }
    # rodar de novo (já tabelas) é idempotente
    TransformRunner(raw_schema="tesouro_direto", lake=lake).run_transform_toml(
        conn, MARTS
    )
    assert _q(conn, "mart_microdata_totais", "1") == [(6, 3, 5)]
