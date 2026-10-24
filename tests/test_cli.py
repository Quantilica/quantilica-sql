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
    result = runner.invoke(app, ["load", "--help"])
    assert result.exit_code == 0
    assert "--source-path" in result.output
    assert "--competencia" in result.output
    assert "--all" in result.output
    assert "--table" in result.output
    assert "--tablespace" in result.output
    assert "--staging-dir" in result.output
    assert "--force" in result.output
