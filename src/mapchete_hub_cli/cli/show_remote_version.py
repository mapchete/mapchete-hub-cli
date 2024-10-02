import click

from mapchete_hub_cli.client import Client


@click.command(help="Show remote versions.")
@click.pass_context
def show_remote_version(
    ctx: click.Context,
    **kwargs,
):
    click.echo(Client(**ctx.obj).remote_version)
