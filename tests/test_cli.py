"""Testes unitários da CLI e do motor quantilica-sql."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from quantilica_sql.adapters import get_adapter, list_supported_domains
from quantilica_sql.cli import app
from quantilica_sql.config import get_settings

runner = CliRunner()


def test_cli_help():
    """Valida que o comando principal responde com código 0 e exibe ajuda."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Motor unificado de ingestão" in result.output
    assert "status" in result.output
    assert "load" in result.output
    assert "run" in result.output


def test_supported_domains():
    """Valida o registro de fontes conhecidas."""
    domains = list_supported_domains()
    assert "sidra" in domains
    assert "bcb" in domains
    assert "pdet" in domains
    assert "rfb" in domains
    assert "cvm" in domains
    assert "inmet" in domains
    assert "anp" in domains
    assert "comex" in domains
    assert "tesouro_direto" in domains
    assert "rtn" in domains
    assert "anac" in domains
    assert "b3" in domains
    assert "datasus" in domains
    assert "inep" in domains
    assert "tse" in domains


def test_adapter_factory():
    """Valida obtenção e schemas dos adaptadores."""
    pdet = get_adapter("pdet")
    assert pdet.domain == "pdet"
    assert pdet.schema == "pdet"

    rfb = get_adapter("rfb-cnpj")
    assert rfb.domain == "rfb"
    assert rfb.schema == "rfb_cnpj"

    sidra = get_adapter("sidra")
    assert sidra.schema == "ibge_sidra"


def test_settings_normalization():
    """Valida a normalização da URL do banco de dados."""
    settings = get_settings()
    url = settings.normalize_db_url()
    assert not url.startswith("postgresql+psycopg://")


def test_cli_load_flags():
    """Valida flags de carga no comando 'load'."""
    import re

    result = runner.invoke(app, ["load", "--help"], env={"COLUMNS": "120"})
    assert result.exit_code == 0
    clean = re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", result.output)
    assert "source-path" in clean
    assert "competencia" in clean
    assert "all" in clean
    assert "table" in clean
    assert "tablespace" in clean
    assert "staging-dir" in clean
    assert "force" in clean


def test_cli_validate_catalogo_real_e_falha_em_manifest_incompleto(tmp_path):
    from pathlib import Path

    from typer.testing import CliRunner

    from quantilica_sql.cli import app

    runner = CliRunner()
    catalogo = Path(__file__).resolve().parents[2] / "quantilica-pipelines"
    if (catalogo / "manifest.toml").exists():
        ok = runner.invoke(app, ["validate", str(catalogo), "--strict"])
        assert ok.exit_code == 0, ok.output

    (tmp_path / "manifest.toml").write_text(
        '[catalog]\nname = "x"\n\n[pipelines.a]\npath = "a"\n', encoding="utf-8"
    )
    res = runner.invoke(app, ["validate", str(tmp_path)])
    assert res.exit_code == 1


def test_cli_lake_convert_e_status(tmp_path, write_hive, monkeypatch):
    from typer.testing import CliRunner

    from quantilica_sql.cli import app

    class Conversor:
        def convert(self, input_dir, lake_dir, *, datasets=None, force=False):
            write_hive(
                lake_dir,
                "tesouro-direto",
                "operacoes-do-tesouro-direto",
                "Codigo do Investidor;Valor da Operacao\n1;10,5\n",
                key="operacoes-do-tesouro-direto-de-2019",
                partition="2019",
                alias="operacoes",
            )
            return type("R", (), {"converted": 1, "skipped": 0, "errors": []})()

    monkeypatch.setattr(
        "quantilica.core.lake.load_lake_converter",
        lambda source: Conversor() if source == "tesouro-direto" else None,
    )
    runner = CliRunner()
    pasta = tmp_path / "tesouro-direto"
    pasta.mkdir()
    lake = tmp_path / "lake"
    res = runner.invoke(
        app,
        ["lake", "convert", "tesouro-direto", "-s", str(pasta), "--lake", str(lake)],
    )
    assert res.exit_code == 0, res.output
    assert "convertidos" in res.output
    status = runner.invoke(app, ["lake", "status", "--lake", str(lake)])
    assert "operacoes-do-tesouro-direto" in status.output

    desconhecida = runner.invoke(app, ["lake", "convert", "inexistente"])
    assert desconhecida.exit_code == 1
    assert "Fonte sem conversor instalado" in desconhecida.output


def test_get_adapter_avisa_adaptadores_obsoletos():
    import warnings

    from quantilica_sql.adapters import get_adapter

    for dominio in ("pdet", "rfb-cnpj", "tesouro-direto", "rtn", "datasus"):
        with pytest.warns(FutureWarning, match="obsoleto"):
            get_adapter(dominio)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        get_adapter("anp")
