"""CLI principal do motor quantilica-sql (montável em quantilica-cli)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from quantilica_sql import __version__
from quantilica_sql.adapters import get_adapter, list_supported_domains
from quantilica_sql.core.tablespaces import list_tablespaces
from quantilica_sql.database import get_connection, inspect_schemas
from quantilica_sql.plugin_manager import PluginManager
from quantilica_sql.runner import run_pipeline
from quantilica_sql.scaffold import scaffold_catalog, scaffold_pipeline

app = typer.Typer(
    name="sql",
    help="Motor unificado de ingestão e pipelines analíticos no PostgreSQL.",
    no_args_is_help=True,
)

plugin_app = typer.Typer(help="Gerenciamento de repositórios de pipelines (plugins).")
app.add_typer(plugin_app, name="plugin")

maint_app = typer.Typer(help="Tarefas de manutenção, índices e tablespaces.")
app.add_typer(maint_app, name="maintenance")

console = Console()


@app.callback()
def main_callback(
    version: bool = typer.Option(
        False, "--version", "-V", help="Exibe versão do quantilica-sql e encerra."
    ),
) -> None:
    """Motor PostgreSQL da Quantilica."""
    if version:
        console.print(f"quantilica-sql v{__version__}")
        raise typer.Exit()


@app.command("status")
def cmd_status() -> None:
    """Exibe o raio-x consolidado de schemas, tamanhos e tabelas no PostgreSQL."""
    try:
        with get_connection() as conn:
            schemas = inspect_schemas(conn)
            table = Table(title="Schemas no PostgreSQL (Banco 'dados')")
            table.add_column("Schema", style="cyan", no_wrap=True)
            table.add_column("Tabelas", justify="right", style="green")
            table.add_column("Tamanho Total", justify="right", style="magenta")

            for s in schemas:
                table.add_row(s["schema"], str(s["table_count"]), s["size_pretty"])

            console.print(table)
    except Exception as exc:
        console.print(f"[red]Erro ao conectar ao PostgreSQL:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command("load")
def cmd_load(
    domain: Annotated[
        str,
        typer.Argument(
            help=f"Domínio a carregar ({', '.join(list_supported_domains())})"
        ),
    ],
    source_path: Annotated[
        str | None,
        typer.Option("--source-path", "-s", help="Caminho do arquivo ou diretório"),
    ] = None,
    competencia: Annotated[
        str | None,
        typer.Option("--competencia", "-c", help="Competência (YYYY-MM ou YYYY)"),
    ] = None,
) -> None:
    """Executa a rotina de carga para uma fonte de dados no PostgreSQL."""
    try:
        adapter = get_adapter(domain)
        console.print(
            f"[cyan]Iniciando carga do domínio:[/cyan] {adapter.domain} "
            f"[dim](schema: {adapter.schema})[/dim]"
        )
        path_obj = Path(source_path) if source_path else None
        with get_connection(autocommit=True) as conn:
            rows = adapter.load(conn, path_obj, competencia=competencia)
            console.print(
                f"[green]Carga concluída![/green] {rows:,} linhas processadas."
            )
    except Exception as exc:
        console.print(f"[red]Falha na carga do domínio '{domain}':[/red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command("run")
def cmd_run(
    pipeline: Annotated[
        str,
        typer.Argument(help="Id, nome ou caminho do pipeline (ex: macro/juros)"),
    ],
    raw_schema: Annotated[
        str | None,
        typer.Option(
            "--raw-schema",
            help="Schema raw no search_path (default: inferido do fetch.toml)",
        ),
    ] = None,
) -> None:
    """Executa um pipeline de transformação SQL na camada analytics."""
    console.print(f"[cyan]Executando pipeline analítico:[/cyan] {pipeline}")
    try:
        res = run_pipeline(pipeline, raw_schema=raw_schema)
        console.print(
            f"[green]Pipeline '{pipeline}' executado com sucesso![/green] "
            f"Materializados: {', '.join(res['materialized'])}"
        )
    except Exception as exc:
        console.print(f"[red]Erro ao executar pipeline '{pipeline}':[/red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command("tablespaces")
def cmd_tablespaces() -> None:
    """Lista as tablespaces configuradas na instância PostgreSQL."""
    try:
        with get_connection() as conn:
            ts_list = list_tablespaces(conn)
            table = Table(title="Tablespaces PostgreSQL")
            table.add_column("Nome", style="cyan")
            table.add_column("Local Físico", style="yellow")
            table.add_column("Tamanho", justify="right", style="magenta")

            for ts in ts_list:
                table.add_row(ts["name"], ts["location"], ts["size_pretty"])

            console.print(table)
    except Exception as exc:
        console.print(f"[red]Erro ao consultar tablespaces:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@plugin_app.command("list")
def cmd_plugin_list() -> None:
    """Lista catálogos de pipelines declarativos instalados."""
    mgr = PluginManager()
    plugins = mgr.list_plugins()
    if not plugins:
        console.print("[dim]Nenhum plugin de pipeline instalado.[/dim]")
        return

    table = Table(title="Plugins de Pipelines Instalados")
    table.add_column("Alias", style="cyan")
    table.add_column("Caminho Local", style="dim")
    for p in plugins:
        table.add_row(p["alias"], p["path"])
    console.print(table)


@plugin_app.command("install")
def cmd_plugin_install(
    url: Annotated[str, typer.Argument(help="URL Git do repositório de pipelines")],
    alias: Annotated[
        str | None,
        typer.Option("--alias", "-a", help="Nome do alias para o catálogo"),
    ] = None,
) -> None:
    """Instala ou atualiza um catálogo de pipelines via Git."""
    mgr = PluginManager()
    try:
        target = mgr.install(url, alias)
        console.print(f"[green]Plugin instalado com sucesso em:[/green] {target}")
    except Exception as exc:
        console.print(f"[red]Erro ao instalar plugin:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@plugin_app.command("remove")
def cmd_plugin_remove(
    alias: Annotated[str, typer.Argument(help="Alias do plugin a remover")],
) -> None:
    """Remove um catálogo de pipelines instalado."""
    mgr = PluginManager()
    try:
        removed = mgr.remove(alias)
        if removed:
            console.print(f"[green]Plugin '{alias}' removido com sucesso.[/green]")
        else:
            console.print(f"[yellow]Plugin '{alias}' não encontrado.[/yellow]")
    except Exception as exc:
        console.print(f"[red]Erro ao remover plugin:[/red] {exc}")
        raise typer.Exit(code=1) from exc


@app.command("scaffold")
def cmd_scaffold(
    name: Annotated[str, typer.Argument(help="Nome do pipeline ou catálogo")],
    type: Annotated[
        str,
        typer.Option(
            "--type",
            "-t",
            help="Tipo de pipeline: ibge, bcb, general (padrão: general)",
        ),
    ] = "general",
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", "-o", help="Diretório de saída (padrão: cwd)"),
    ] = None,
    catalog: Annotated[
        bool,
        typer.Option(
            "--catalog", help="Criar catálogo completo com manifest.toml e README.md"
        ),
    ] = False,
    description: Annotated[
        str,
        typer.Option(
            "--description", "-d", help="Descrição do catálogo (com --catalog)"
        ),
    ] = "",
) -> None:
    """Gera scaffold de pipeline ou catálogo declarativo.

    Exemplos:
      quantilica-sql scaffold meu_pipeline --type ibge
      quantilica-sql scaffold meu_catalogo --catalog --description "Meu catálogo"
    """
    target_dir = output_dir or Path.cwd()
    if catalog:
        console.print(f"[cyan]Criando catálogo:[/cyan] {name}")
        try:
            path = scaffold_catalog(target_dir, name, description)
            console.print(f"[green]Catálogo criado em:[/green] {path}")
        except Exception as exc:
            console.print(f"[red]Erro ao criar catálogo:[/red] {exc}")
            raise typer.Exit(code=1) from exc
    else:
        console.print(f"[cyan]Criando pipeline ({type}):[/cyan] {name}")
        try:
            path = scaffold_pipeline(target_dir, name, type)
            console.print(f"[green]Pipeline criado em:[/green] {path}")
        except ValueError as exc:
            console.print(f"[red]Erro de validação:[/red] {exc}")
            raise typer.Exit(code=1) from exc
        except Exception as exc:
            console.print(f"[red]Erro ao criar pipeline:[/red] {exc}")
            raise typer.Exit(code=1) from exc


def main() -> None:
    """Entry point standalone para a CLI quantilica-sql."""
    app()


if __name__ == "__main__":
    main()
