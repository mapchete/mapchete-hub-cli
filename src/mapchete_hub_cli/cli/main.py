from importlib import metadata

import click
from click_plugins import with_plugins

from mapchete_hub_cli import DEFAULT_TIMEOUT, __version__


entry_points = metadata.entry_points()
commands = entry_points.select(group="mapchete_hub_cli.cli.commands")

host_options = dict(host_ip="0.0.0.0", port=5000)


@with_plugins(commands)
@click.version_option(version=__version__, message="%(version)s")
@click.group(help="Process control on Mapchete Hub.")
@click.option(
    "--host",
    "-h",
    type=click.STRING,
    nargs=1,
    default=f"{host_options['host_ip']}:{host_options['port']}",
    help="Address and port of mhub endpoint",
    show_default=True,
)
@click.option(
    "--timeout",
    type=click.INT,
    default=DEFAULT_TIMEOUT,
    help="Time in seconds to wait for server response.",
    show_default=True,
)
@click.option(
    "--user",
    "-u",
    type=click.STRING,
    help="Username for basic auth. (Or set MHUB_USER env variable.)",
)
@click.option(
    "--password",
    "-p",
    type=click.STRING,
    help="Password for basic auth. (Or set MHUB_PASSWORD env variable.)",
)
@click.pass_context
def mhub(ctx, host, **kwargs):
    """Main command group."""
    host = host if host.startswith("http") else f"http://{host}"
    host = host if host.endswith("/") else f"{host}/"
    ctx.obj = dict(host=host, **kwargs)
