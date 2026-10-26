"""Testes unitários da CLI e do motor quantilica-sql."""

from __future__ import annotations

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


def test_cli_lake_convert_e_status(tmp_path):
    import pytest
    from typer.testing import CliRunner

    pytest.importorskip("duckdb")
    from quantilica_sql.cli import app

    runner = CliRunner()
    pasta = tmp_path / "tesouro-direto" / "operacoes-do-tesouro-direto"
    pasta.mkdir(parents=True)
    (pasta / "operacoes-do-tesouro-direto@20261016T000000.csv").write_text(
        "Codigo do Investidor;Valor da Operacao\n1;10,5\n", encoding="utf-8"
    )
    lake = tmp_path / "lake"
    args = ["lake", "convert", "tesouro-direto", "-s", str(tmp_path / "tesouro-direto")]
    res = runner.invoke(app, [*args, "--lake", str(lake)])
    assert res.exit_code == 0, res.output
    assert "1 convertidos" in " ".join(res.output.split())
    again = runner.invoke(app, [*args, "--lake", str(lake)])
    saida = " ".join(again.output.split())
    assert "0 convertidos" in saida and "1 sem mudança" in saida
    status = runner.invoke(app, ["lake", "status", "--lake", str(lake)])
    assert "operacoes-do-tesouro-direto" in status.output

    desconhecida = runner.invoke(app, ["lake", "convert", "inexistente"])
    assert desconhecida.exit_code == 2
