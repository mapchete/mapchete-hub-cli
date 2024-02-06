import click

from mapchete_hub_cli.cli import options
from mapchete_hub_cli.client import Client


@click.command(short_help="Show available processes.")
@click.option(
    "--process-name", "-n", type=click.STRING, help="Print docstring of process."
)
@click.option("--docstrings", is_flag=True, help="Print docstrings of all processes.")
@options.opt_debug
@click.pass_context
def processes(ctx, process_name=None, docstrings=False, debug=None, **kwargs):
    """Show available processes."""

    def _print_process_info(process_module, docstrings=False):
        click.echo(
            click.style(process_module["title"], bold=docstrings, underline=docstrings)
        )
        if docstrings:
            click.echo(process_module["description"])

    try:
        client = Client(**ctx.obj)
        res = client.get("processes")
        if res.status_code != 200:  # pragma: no cover
            raise ConnectionError(res.json())

        # get all registered processes
        processes = {p.get("title"): p for p in res.json().get("processes")}

        # print selected process
        if process_name:
            _print_process_info(processes[process_name], docstrings=True)
        else:
            # print all processes
            click.echo(f"{len(processes)} processes found")
            for process_name in sorted(processes.keys()):
                _print_process_info(processes[process_name], docstrings=docstrings)
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)
