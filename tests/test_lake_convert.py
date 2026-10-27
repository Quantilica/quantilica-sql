"""Conversão para o lake: Parquet em streaming, idempotência por hash e republicação."""

from __future__ import annotations

import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

from quantilica_sql.lake import (  # noqa: E402
    convert_file,
    convert_tree,
    file_key,
    get_spec,
    normalize_name,
    partition_of,
)
from quantilica_sql.lake.specs import unique_names  # noqa: E402


def _rows(parquet: Path) -> list[tuple]:
    return (
        duckdb.connect()
        .execute(f"SELECT * FROM read_parquet('{parquet}') ORDER BY 1")
        .fetchall()
    )


def _cols(parquet: Path) -> list[str]:
    cur = duckdb.connect().execute(f"SELECT * FROM read_parquet('{parquet}') LIMIT 0")
    return [d[0] for d in cur.description]


def test_normalize_name_e_chave_de_arquivo():
    assert normalize_name("Código do Investidor") == "codigo_do_investidor"
    assert normalize_name("﻿Operou 12 Meses") == "operou_12_meses"
    assert unique_names(["A b", "a_b", "A-B"]) == ["a_b", "a_b_2", "a_b_3"]
    assert file_key("rais-vinculos_2025-sul@20260513.7z") == "rais-vinculos_2025-sul"
    assert file_key("Empresas0@20260914.zip") == "Empresas0"
    assert file_key("investidores-do-tesouro-direto-de-2019@20261007T000000.csv") == (
        "investidores-do-tesouro-direto-de-2019"
    )
    assert file_key("dados.csv.zip") == "dados"


def test_particao_vem_da_pasta_de_ano_ou_competencia(tmp_path):
    raiz = tmp_path / "pdet"
    assert partition_of(raiz / "rais-vinculos" / "2025" / "x.7z", raiz) == "2025"
    assert partition_of(raiz / "2026-09" / "empresas" / "x.zip", raiz) == "2026-09"
    assert partition_of(raiz / "tesouro-direto" / "x.csv", raiz) == ""


def test_csv_latin1_com_cabecalho_vira_parquet_de_texto_normalizado(tmp_path):
    spec = get_spec("pdet", "rais-vinculos")
    src = tmp_path / "rais-vinculos_2025@20260513.csv"
    src.write_bytes(
        "Município;Vl Remun Média (R$)\nSão Paulo;1.234,50\nBelém;99\n".encode(
            "latin-1"
        )
    )
    res = convert_file(spec, src, partition="2025", lake=tmp_path / "lake")
    assert res.status == "converted" and res.rows == 2
    assert (
        res.output
        == tmp_path / "lake/pdet/rais-vinculos/2025/rais-vinculos_2025.parquet"
    )
    assert _cols(res.output) == ["municipio", "vl_remun_media_r"]
    assert _rows(res.output) == [
        ("Belém", "99"),
        ("São Paulo", "1.234,50"),
    ]  # texto, sem tipar


def test_mesmo_hash_nao_reconverte_e_force_reconverte(tmp_path):
    spec = get_spec("pdet", "caged")
    src = tmp_path / "caged_202601@20260201.csv"
    src.write_text("a;b\n1;2\n", encoding="latin-1")
    lake = tmp_path / "lake"
    assert convert_file(spec, src, lake=lake).status == "converted"
    assert convert_file(spec, src, lake=lake).status == "skipped"
    assert convert_file(spec, src, lake=lake, force=True).status == "converted"


def test_republicacao_com_data_nova_no_nome_substitui_o_parquet(tmp_path):
    spec = get_spec("pdet", "caged")
    lake = tmp_path / "lake"
    v1 = tmp_path / "caged_202601@20260201.csv"
    v1.write_text("a;b\n1;2\n", encoding="latin-1")
    out1 = convert_file(spec, v1, lake=lake).output
    v2 = tmp_path / "caged_202601@20260301.csv"
    v2.write_text("a;b\n1;2\n3;4\n", encoding="latin-1")
    res = convert_file(spec, v2, lake=lake)
    assert res.output == out1 and res.status == "converted"
    assert _rows(res.output) == [("1", "2"), ("3", "4")]
    assert len(list(lake.rglob("*.parquet"))) == 1


def test_zip_sem_cabecalho_usa_os_nomes_do_layout_oficial(tmp_path):
    spec = get_spec("rfb-cnpj", "empresas")
    z = tmp_path / "Empresas0@20260914.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr(
            "K3241.K03200Y0.D60914.EMPRECSV",
            '"00000000";"BANCO DO BRASIL SA";"2038";"10";"1000000,00";"05";""\n'
            '"00000001";"ACME LTDA";"2062";"49";"5000,00";"01";""\n',
        )
    res = convert_file(spec, z, partition="2026-09", lake=tmp_path / "lake")
    assert res.rows == 2
    assert _cols(res.output) == list(spec.names)
    assert _rows(res.output)[0][:3] == ("00000000", "BANCO DO BRASIL SA", "2038")


@pytest.mark.skipif(shutil.which("7z") is None, reason="7z ausente")
def test_arquivo_7z(tmp_path):
    spec = get_spec("pdet", "rais-vinculos")
    txt = tmp_path / "dados.txt"
    txt.write_bytes("Município;Valor\nOlinda;1\n".encode("latin-1"))
    arq = tmp_path / "rais-vinculos_2025-nordeste@20260513.7z"
    subprocess.run(
        ["7z", "a", "-bd", str(arq), str(txt)], check=True, capture_output=True
    )
    res = convert_file(spec, arq, partition="2025", lake=tmp_path / "lake")
    assert _rows(res.output) == [("Olinda", "1")]


def test_alias_de_coluna_do_tesouro_direto(tmp_path):
    spec = get_spec("tesouro-direto", "investidores-do-tesouro-direto")
    src = tmp_path / "investidores-do-tesouro-direto-de-2019@20261007T000000.csv"
    src.write_text(
        "Codigo do Investidor;Data de Adesao;Idade;UF do Investidor\n"
        "1;01/01/2019;30;SP\n",
        encoding="utf-8",
    )
    res = convert_file(spec, src, lake=tmp_path / "lake")
    assert _cols(res.output) == [
        "codigo_do_investidor",
        "data_de_adesao",
        "idade_do_investidor",
        "uf_do_investidor",
    ]


def test_pagina_de_erro_no_lugar_do_csv_e_rejeitada(tmp_path):
    from quantilica_sql.lake.convert import LakeError

    spec = get_spec("tesouro-direto", "operacoes-do-tesouro-direto")
    src = tmp_path / "operacoes-do-tesouro-direto-de-2018@20261006T131641.csv"
    src.write_text(
        "<html><head><title>Request Rejected</title></head><body>x</body></html>\n"
    )
    lake = tmp_path / "lake"
    with pytest.raises(LakeError, match="cabeçalho inesperado"):
        convert_file(spec, src, lake=lake)
    assert not list(lake.rglob("*.parquet"))


def test_convert_tree_isola_o_arquivo_ruim_e_ignora_sidecar(tmp_path):
    raiz = tmp_path / "pdet"
    ds = raiz / "caged" / "2026"
    ds.mkdir(parents=True)
    (ds / "bom@20260101.csv").write_text("a;b\n1;2\n", encoding="latin-1")
    (ds / "bom@20260101.csv.manifest.json").write_text("{}")
    (ds / "ruim@20260101.zip").write_bytes(b"isto nao e um zip")
    (raiz / "desconhecido").mkdir()
    (raiz / "desconhecido" / "x.csv").write_text("a\n1\n")
    s = convert_tree("pdet", raiz, lake=tmp_path / "lake")
    assert (s.converted, s.skipped) == (1, 0)
    assert [p.name for p, _ in s.errors] == ["ruim@20260101.zip"]
    assert not s.ok
    again = convert_tree("pdet", raiz, lake=tmp_path / "lake")
    assert (again.converted, again.skipped) == (0, 1)


# --- consumo do lake Hive pelos descritores -----------------------------------------


def test_connect_lake_le_o_hive_com_apelido_e_tipos_do_descritor(tmp_path, write_hive):
    from quantilica_sql.lake.duck import connect_lake

    lake = tmp_path / "lake"
    write_hive(
        lake,
        "tesouro-direto",
        "investidores-do-tesouro-direto",
        "Codigo do Investidor;UF\n1;SP\n",
        key="inv-2018",
        partition="2018",
        alias="investidores",
        partition_types={"periodo": "int"},
    )
    write_hive(
        lake,
        "tesouro-direto",
        "investidores-do-tesouro-direto",
        "Codigo do Investidor;UF\n2;RJ\n",
        key="inv-2019",
        partition="2019",
        alias="investidores",
        partition_types={"periodo": "int"},
    )
    duck = connect_lake(lake)
    rows = duck.execute(
        "SELECT periodo, codigo_do_investidor, _arquivo "
        "FROM tesouro_direto.investidores ORDER BY 1"
    ).fetchall()
    assert rows == [(2018, "1", "inv-2018.parquet"), (2019, "2", "inv-2019.parquet")]
    described = duck.execute(
        "DESCRIBE SELECT periodo FROM tesouro_direto.investidores"
    ).fetchall()
    tipos = {r[0]: r[1] for r in described}
    assert tipos == {"periodo": "BIGINT"}


def test_connect_lake_mantem_zero_a_esquerda_da_particao_varchar(tmp_path, write_hive):
    from quantilica_sql.lake.duck import connect_lake

    lake = tmp_path / "lake"
    write_hive(
        lake,
        "pdet",
        "caged",
        "a;b\n1;2\n",
        key="caged_202601",
        partition="01",
        partition_key="mes",
        partition_types={"mes": "str"},
    )
    duck = connect_lake(lake)
    assert duck.execute("SELECT mes FROM pdet.caged").fetchall() == [("01",)]


def test_connect_lake_sem_descritor_usa_o_nome_do_dataset_normalizado(
    tmp_path, write_hive
):
    from quantilica_sql.lake.duck import connect_lake

    lake = tmp_path / "lake"
    write_hive(
        lake,
        "rfb-cnpj",
        "dados-abertos",
        "a;b\n1;2\n",
        key="x",
        partition="2026-09",
        descriptor=False,
    )
    duck = connect_lake(lake)
    assert duck.execute(
        "SELECT a, b, periodo FROM rfb_cnpj.dados_abertos"
    ).fetchall() == [("1", "2", "2026-09")]


def test_connect_lake_rejeita_descritor_invalido(tmp_path, write_hive):
    from quantilica_sql.lake.convert import LakeError
    from quantilica_sql.lake.duck import connect_lake

    lake = tmp_path / "lake"
    out = write_hive(lake, "pdet", "caged", "a\n1\n", key="x", partition="2026")
    (out.parent.parent / "_dataset.json").write_text("{ não é json")
    with pytest.raises(LakeError, match="_dataset.json inválido"):
        connect_lake(lake)


# --- `lake convert` delega ao conversor do entry point ------------------------------


class _FakeConverter:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def convert(self, input_dir, lake_dir, *, datasets=None, force=False):
        self.calls.append((input_dir, lake_dir, datasets, force))
        return self.result


class _Result:
    def __init__(self, converted=0, skipped=0, errors=()):
        self.converted, self.skipped, self.errors = converted, skipped, list(errors)


def _convert_cli(monkeypatch, converter, *args):
    from typer.testing import CliRunner

    from quantilica_sql.cli import app

    monkeypatch.setattr(
        "quantilica.core.lake.load_lake_converter", lambda source: converter
    )
    return CliRunner().invoke(app, ["lake", "convert", *args])


def test_lake_convert_delega_ao_conversor_e_resume(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    fake = _FakeConverter(_Result(converted=2, skipped=1))
    res = _convert_cli(
        monkeypatch,
        fake,
        "tesouro-direto",
        "-s",
        str(raw),
        "--lake",
        str(tmp_path / "lake"),
        "--dataset",
        "investidores",
        "--force",
    )
    assert res.exit_code == 0, res.output
    assert fake.calls == [(raw, tmp_path / "lake", ["investidores"], True)]
    assert "convertidos" in res.output and "2" in res.output


def test_lake_convert_sem_dataset_converte_todos(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    fake = _FakeConverter(_Result())
    res = _convert_cli(
        monkeypatch, fake, "pdet", "-s", str(raw), "--lake", str(tmp_path / "l")
    )
    assert res.exit_code == 0, res.output
    assert fake.calls == [(raw, tmp_path / "l", None, False)]


def test_lake_convert_com_erros_sai_com_codigo_1(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    fake = _FakeConverter(_Result(converted=1, errors=["arquivo [x] corrompido"]))
    res = _convert_cli(
        monkeypatch, fake, "pdet", "-s", str(raw), "--lake", str(tmp_path / "l")
    )
    assert res.exit_code == 1
    assert "arquivo [x] corrompido" in res.output


def test_lake_convert_sem_conversor_instalado_orienta_a_instalar(tmp_path, monkeypatch):
    res = _convert_cli(monkeypatch, None, "fonte-x", "-s", str(tmp_path))
    assert res.exit_code == 1
    saida = " ".join(res.output.split())  # o Rich quebra linhas em 80 colunas
    assert "Fonte sem conversor instalado" in saida
    assert "quantilica install fonte-x --extra analysis" in saida


def test_lake_convert_pasta_de_origem_inexistente(tmp_path, monkeypatch):
    fake = _FakeConverter(_Result())
    res = _convert_cli(monkeypatch, fake, "pdet", "-s", str(tmp_path / "nao-existe"))
    assert res.exit_code == 1
    assert not fake.calls
