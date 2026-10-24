"""Testes unitários para scaffold.py e comandos CLI de plugin."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from quantilica_sql.cli import app
from quantilica_sql.scaffold import scaffold_catalog, scaffold_pipeline
from quantilica_sql.validator import PipelineValidator

runner = CliRunner()


# --------------------------------------------------------------------------
# Testes de scaffold_pipeline
# --------------------------------------------------------------------------


def test_scaffold_pipeline_ibge(tmp_path: Path):
    """Valida scaffold de pipeline tipo 'ibge'."""
    path = scaffold_pipeline(tmp_path, "meu_ipca", pipeline_type="ibge")
    assert path.exists()
    assert path.name == "meu_ipca"
    assert (path / "fetch.toml").exists()
    assert (path / "transform.toml").exists()
    assert (path / "meu_ipca.sql").exists()

    # Validação estrutural
    validator = PipelineValidator()
    report = validator.validate_pipeline_dir(path)
    assert report.ok, f"Erros: {[str(i) for i in report.errors]}"

    # Conteúdo básico do fetch.toml
    fetch_content = (path / "fetch.toml").read_text(encoding="utf-8")
    assert "tabela_sidra" in fetch_content
    assert "[[tabelas]]" in fetch_content

    # Conteúdo básico do transform.toml
    transform_content = (path / "transform.toml").read_text(encoding="utf-8")
    assert 'name        = "meu_ipca"' in transform_content
    assert 'schema      = "analytics"' in transform_content
    assert 'strategy    = "replace"' in transform_content
    assert 'sql         = "meu_ipca.sql"' in transform_content

    # Conteúdo básico do .sql
    sql_content = (path / "meu_ipca.sql").read_text(encoding="utf-8")
    assert "SELECT" in sql_content
    assert "ibge_sidra" in sql_content.lower()


def test_scaffold_pipeline_bcb(tmp_path: Path):
    """Valida scaffold de pipeline tipo 'bcb'."""
    path = scaffold_pipeline(tmp_path, "minhas_taxas", pipeline_type="bcb")
    assert path.exists()
    assert path.name == "minhas_taxas"
    assert (path / "fetch.toml").exists()
    assert (path / "transform.toml").exists()
    assert (path / "minhas_taxas.sql").exists()

    # Validação estrutural
    validator = PipelineValidator()
    report = validator.validate_pipeline_dir(path)
    assert report.ok, f"Erros: {[str(i) for i in report.errors]}"

    # Conteúdo básico do fetch.toml
    fetch_content = (path / "fetch.toml").read_text(encoding="utf-8")
    assert "[[series]]" in fetch_content
    assert "ids = [432, 11, 12, 226]" in fetch_content

    # Conteúdo básico do .sql
    sql_content = (path / "minhas_taxas.sql").read_text(encoding="utf-8")
    assert "series_data" in sql_content
    assert "series_metadata" in sql_content


def test_scaffold_pipeline_general(tmp_path: Path):
    """Valida scaffold de pipeline tipo 'general' (cross-domain)."""
    path = scaffold_pipeline(tmp_path, "meu_cross", pipeline_type="general")
    assert path.exists()
    assert path.name == "meu_cross"

    # Validação estrutural
    validator = PipelineValidator()
    report = validator.validate_pipeline_dir(path)
    assert report.ok, f"Erros: {[str(i) for i in report.errors]}"

    # Conteúdo básico do fetch.toml
    fetch_content = (path / "fetch.toml").read_text(encoding="utf-8")
    assert "[pipeline]" in fetch_content
    assert 'domains = ["ibge_sidra", "bcb_sgs"]' in fetch_content

    # Conteúdo básico do .sql
    sql_content = (path / "meu_cross.sql").read_text(encoding="utf-8")
    assert "FULL OUTER JOIN" in sql_content


def test_scaffold_pipeline_invalid_type(tmp_path: Path):
    """Valida erro para tipo de pipeline inválido."""
    with pytest.raises(ValueError, match="pipeline_type inválido"):
        scaffold_pipeline(tmp_path, "teste", pipeline_type="invalido")  # type: ignore[arg-type]


def test_scaffold_pipeline_slugification(tmp_path: Path):
    """Valida que nomes com espaços/caracteres especiais são slugificados."""
    path = scaffold_pipeline(
        tmp_path, "Meu Pipeline Complexo!", pipeline_type="general"
    )
    assert path.name == "meu_pipeline_complexo"
    assert (path / "meu_pipeline_complexo.sql").exists()


# --------------------------------------------------------------------------
# Testes de scaffold_catalog
# --------------------------------------------------------------------------


def test_scaffold_catalog(tmp_path: Path):
    """Valida scaffold de catálogo completo."""
    path = scaffold_catalog(tmp_path, "meu_catalogo", "Descrição de teste")
    assert path.exists()
    assert path.name == "meu_catalogo"
    assert (path / "manifest.toml").exists()
    assert (path / "README.md").exists()
    assert (path / "main").is_dir()
    assert (path / "main" / "fetch.toml").exists()
    assert (path / "main" / "transform.toml").exists()
    assert (path / "main" / "main.sql").exists()

    # Validação completa do catálogo
    validator = PipelineValidator()
    report = validator.validate_catalog(path)
    assert report.ok, f"Erros: {[str(i) for i in report.errors]}"

    # Conteúdo do manifest.toml
    manifest_content = (path / "manifest.toml").read_text(encoding="utf-8")
    assert 'name = "meu_catalogo"' in manifest_content
    assert 'version = "1.0.0"' in manifest_content
    assert "Descrição de teste" in manifest_content
    assert "[pipelines.meu_catalogo]" in manifest_content
    assert 'path = "main"' in manifest_content

    # Conteúdo do README.md
    readme_content = (path / "README.md").read_text(encoding="utf-8")
    assert "# meu_catalogo" in readme_content
    assert "Descrição de teste" in readme_content


def test_scaffold_catalog_without_description(tmp_path: Path):
    """Valida scaffold de catálogo sem descrição."""
    path = scaffold_catalog(tmp_path, "catalogo_simples")
    assert path.exists()
    manifest_content = (path / "manifest.toml").read_text(encoding="utf-8")
    assert "Catálogo de pipelines: catalogo_simples" in manifest_content


def test_scaffold_catalog_slugification(tmp_path: Path):
    """Valida slugificação do nome do catálogo."""
    path = scaffold_catalog(tmp_path, "Meu Catálogo Completo")
    # slugify converte acentos para underscore
    assert path.name == "meu_cat_logo_completo"


# --------------------------------------------------------------------------
# Testes CLI: scaffold
# --------------------------------------------------------------------------


def test_cli_scaffold_pipeline(tmp_path: Path, monkeypatch):
    """Testa comando CLI: quantilica-sql scaffold <name> --type ibge."""
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["scaffold", "teste_pipeline", "--type", "ibge"])
    assert result.exit_code == 0, result.output
    assert "Pipeline criado em:" in result.output
    assert (tmp_path / "teste_pipeline" / "fetch.toml").exists()
    assert (tmp_path / "teste_pipeline" / "transform.toml").exists()
    assert (tmp_path / "teste_pipeline" / "teste_pipeline.sql").exists()


def test_cli_scaffold_pipeline_bcb(tmp_path: Path, monkeypatch):
    """Testa comando CLI: quantilica-sql scaffold <name> --type bcb."""
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["scaffold", "teste_bcb", "--type", "bcb"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "teste_bcb" / "fetch.toml").exists()
    fetch_content = (tmp_path / "teste_bcb" / "fetch.toml").read_text()
    assert "[[series]]" in fetch_content


def test_cli_scaffold_pipeline_general(tmp_path: Path, monkeypatch):
    """Testa comando CLI: quantilica-sql scaffold <name> --type general."""
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["scaffold", "teste_gen", "--type", "general"])
    assert result.exit_code == 0, result.output
    fetch_content = (tmp_path / "teste_gen" / "fetch.toml").read_text()
    assert "[pipeline]" in fetch_content


def test_cli_scaffold_pipeline_invalid_type(tmp_path: Path, monkeypatch):
    """Testa erro CLI para tipo inválido."""
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["scaffold", "teste", "--type", "invalido"])
    assert result.exit_code != 0
    assert "pipeline_type inválido" in result.output or "Erro" in result.output


def test_cli_scaffold_catalog(tmp_path: Path, monkeypatch):
    """Testa comando CLI: quantilica-sql scaffold <name> --catalog."""
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app, ["scaffold", "meu_cat", "--catalog", "--description", "Desc teste"]
    )
    assert result.exit_code == 0, result.output
    assert "Catálogo criado em:" in result.output
    assert (tmp_path / "meu_cat" / "manifest.toml").exists()
    assert (tmp_path / "meu_cat" / "README.md").exists()
    assert (tmp_path / "meu_cat" / "main" / "fetch.toml").exists()


def test_cli_scaffold_output_dir(tmp_path: Path, monkeypatch):
    """Testa --output-dir."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "saida").mkdir()
    result = runner.invoke(
        app, ["scaffold", "pipeline_dir", "--output-dir", "saida", "--type", "ibge"]
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "saida" / "pipeline_dir" / "fetch.toml").exists()


# --------------------------------------------------------------------------
# Testes CLI: plugin list
# --------------------------------------------------------------------------


def test_cli_plugin_list_empty(tmp_path: Path, monkeypatch):
    """Testa plugin list sem plugins instalados."""
    from quantilica_sql import config, plugin_manager

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True)
    # Reset singleton
    config._settings_instance = None
    # Patch no módulo onde é usado
    monkeypatch.setattr(
        plugin_manager, "get_settings", lambda: config.Settings(plugins_dir=plugins_dir)
    )

    result = runner.invoke(app, ["plugin", "list"])
    assert result.exit_code == 0
    assert "Nenhum plugin" in result.output or "nenhum" in result.output.lower()


def test_cli_plugin_list_with_plugins(tmp_path: Path, monkeypatch):
    """Testa plugin list com plugins instalados."""
    from quantilica_sql import config, plugin_manager

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True)
    # Cria fake plugin
    (plugins_dir / "meu_plugin").mkdir()
    (plugins_dir / "meu_plugin" / "manifest.toml").write_text(
        '[catalog]\nname="teste"\nversion="1.0.0"\n[pipelines.p]\npath="p"\n'
    )

    config._settings_instance = None
    monkeypatch.setattr(
        plugin_manager, "get_settings", lambda: config.Settings(plugins_dir=plugins_dir)
    )

    result = runner.invoke(app, ["plugin", "list"])
    assert result.exit_code == 0
    assert "meu_plugin" in result.output
    assert "Plugins de Pipelines Instalados" in result.output


# --------------------------------------------------------------------------
# Testes CLI: plugin install
# --------------------------------------------------------------------------


def test_cli_plugin_install(tmp_path: Path, monkeypatch):
    """Testa plugin install (mock git clone)."""
    from quantilica_sql import config, plugin_manager

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True)

    config._settings_instance = None
    monkeypatch.setattr(
        plugin_manager, "get_settings", lambda: config.Settings(plugins_dir=plugins_dir)
    )

    # Mock do subprocess.run no módulo plugin_manager
    import subprocess

    original_run = subprocess.run

    def mock_run(cmd, *args, **kwargs):
        if isinstance(cmd, list) and cmd[:2] == ["git", "clone"]:
            target = Path(cmd[-1])
            target.mkdir(parents=True)
            (target / "manifest.toml").write_text(
                '[catalog]\nname="clonado"\nversion="1.0.0"\n[pipelines.p]\npath="p"\n'
            )
            return type("R", (), {"returncode": 0})()
        return original_run(cmd, *args, **kwargs)

    monkeypatch.setattr(plugin_manager.subprocess, "run", mock_run)

    result = runner.invoke(
        app, ["plugin", "install", "https://github.com/user/repo.git"]
    )
    assert result.exit_code == 0, result.output
    assert "instalado com sucesso" in result.output


def test_cli_plugin_install_with_alias(tmp_path: Path, monkeypatch):
    """Testa plugin install com --alias."""
    from quantilica_sql import config, plugin_manager

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True)

    config._settings_instance = None
    monkeypatch.setattr(
        plugin_manager, "get_settings", lambda: config.Settings(plugins_dir=plugins_dir)
    )

    import subprocess

    original_run = subprocess.run

    def mock_run(cmd, *args, **kwargs):
        if isinstance(cmd, list) and cmd[:2] == ["git", "clone"]:
            # git clone <url> <target_dir> - target_dir is 3rd arg (index 2)
            # but if alias is provided, it's git clone <url> <alias_dir>
            # so we need to check the last argument
            target = Path(cmd[-1])
            target.mkdir(parents=True)
            (target / "manifest.toml").write_text(
                '[catalog]\nname="clonado"\nversion="1.0.0"\n[pipelines.p]\npath="p"\n'
            )
            return type("R", (), {"returncode": 0})()
        return original_run(cmd, *args, **kwargs)

    monkeypatch.setattr(plugin_manager.subprocess, "run", mock_run)

    result = runner.invoke(
        app,
        [
            "plugin",
            "install",
            "https://github.com/user/repo.git",
            "--alias",
            "meu_alias",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (plugins_dir / "meu_alias" / "manifest.toml").exists()


# --------------------------------------------------------------------------
# Testes CLI: plugin remove
# --------------------------------------------------------------------------


def test_cli_plugin_remove(tmp_path: Path, monkeypatch):
    """Testa plugin remove."""
    from quantilica_sql import config, plugin_manager

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True)
    (plugins_dir / "plugin_remover").mkdir()
    (plugins_dir / "plugin_remover" / "manifest.toml").write_text(
        '[catalog]\nname="teste"\nversion="1.0.0"\n[pipelines.p]\npath="p"\n'
    )

    config._settings_instance = None
    monkeypatch.setattr(
        plugin_manager, "get_settings", lambda: config.Settings(plugins_dir=plugins_dir)
    )

    result = runner.invoke(app, ["plugin", "remove", "plugin_remover"])
    assert result.exit_code == 0, result.output
    assert "removido com sucesso" in result.output
    assert not (plugins_dir / "plugin_remover").exists()


def test_cli_plugin_remove_not_found(tmp_path: Path, monkeypatch):
    """Testa plugin remove inexistente."""
    from quantilica_sql import config, plugin_manager

    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir(parents=True)

    config._settings_instance = None
    monkeypatch.setattr(
        plugin_manager, "get_settings", lambda: config.Settings(plugins_dir=plugins_dir)
    )

    result = runner.invoke(app, ["plugin", "remove", "nao_existe"])
    assert result.exit_code == 0
    assert "não encontrado" in result.output


# --------------------------------------------------------------------------
# Testes de integração: scaffold + validator
# --------------------------------------------------------------------------


def test_scaffold_integration_full_validation(tmp_path: Path):
    """Teste de integração: scaffold -> validação completa passa."""
    # Pipeline
    p = scaffold_pipeline(tmp_path, "integracao_teste", pipeline_type="ibge")
    v = PipelineValidator()
    assert v.validate_pipeline_dir(p).ok

    # Catálogo
    c = scaffold_catalog(tmp_path, "catalogo_integracao", "Teste integração")
    assert v.validate_catalog(c).ok


def test_scaffold_pipeline_idempotent(tmp_path: Path):
    """Valida que scaffold não sobrescreve arquivos existentes."""
    scaffold_pipeline(tmp_path, "idempotente", pipeline_type="ibge")
    # Modifica o fetch.toml
    fetch = tmp_path / "idempotente" / "fetch.toml"
    original = fetch.read_text()
    fetch.write_text("# MODIFICADO\n" + original)
    # Executa scaffold novamente
    scaffold_pipeline(tmp_path, "idempotente", pipeline_type="ibge")
    # Arquivo não deve ser sobrescrito
    assert fetch.read_text().startswith("# MODIFICADO")
