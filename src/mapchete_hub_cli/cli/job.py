from typing import List, Optional

import click
import oyaml as yaml

from mapchete_hub_cli.cli import options
from mapchete_hub_cli.client import Client, Job
from mapchete_hub_cli.enums import Status
from mapchete_hub_cli.time import pretty_time, pretty_time_passed, str_to_date


@click.command(short_help="Show job status.")
@click.argument("job_id", type=click.STRING)
@options.opt_geojson
@options.opt_metadata_items
@click.option("--traceback", is_flag=True, help="Print only traceback if available.")
@click.option("--show-config", is_flag=True, help="Print Mapchete config.")
@click.option("--show-params", is_flag=True, help="Print Mapchete parameters.")
@click.option("--show-process", is_flag=True, help="Print Mapchete process.")
@options.opt_debug
@click.pass_context
def job(
    ctx,
    job_id,
    geojson=False,
    show_config=False,
    show_params=False,
    show_process=False,
    traceback=False,
    debug=False,
    metadata_items=None,
    **_,
):
    """
    Show job status.

    JOB_ID can either be a valid job ID or :last:, in which case the CLI will automatically
    determine the most recently updated job.
    """
    try:
        client = Client(**ctx.obj)
        job = client.job(job_id)
        if geojson:  # pragma: no cover
            click.echo(job.to_json())
            return
        elif show_config:
            click.echo(yaml.dump(job.properties["mapchete"]["config"], indent=2))
            return
        elif show_params:
            for k, v in job.properties["mapchete"]["params"].items():
                click.echo(
                    f"{k}: {', '.join(map(str, v)) if v else None}"
                ) if isinstance(v, list) else click.echo(f"{k}: {v}")
            return
        elif show_process:
            process = job.properties["mapchete"]["config"].get("process")
            process = process if isinstance(process, list) else [process]
            for line in process:
                click.echo(line)
            return
        elif traceback:  # pragma: no cover
            click.echo(job.properties.get("exception"))
            click.echo(job.properties.get("traceback"))
        print_job_details(job, metadata_items=metadata_items, verbose=True)
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


status_colors = {
    Status.pending: "blue",
    Status.parsing: "yellow",
    Status.initializing: "yellow",
    Status.running: "yellow",
    Status.retrying: "yellow",
    Status.done: "green",
    Status.failed: "red",
    Status.cancelled: "magenta",
}


def print_job_details(
    job: Job, metadata_items: Optional[List[str]] = None, verbose: bool = False
):
    try:
        color = status_colors[job.status]
    except KeyError:  # pragma: no cover
        color = "white"

    mapchete_config = job.properties.get("mapchete", {}).get("config", {})

    # job ID and job status
    click.echo(click.style(f"{job.job_id}", fg=color, bold=True))

    if verbose:
        # job name
        click.echo(f"job name: {job.properties.get('job_name')}")

        # status
        click.echo(click.style(f"status: {job.status}"))

        # exception
        click.echo(click.style(f"exception: {job.properties.get('exception')}"))

        # progress
        current = job.properties.get("current_progress", 0)
        total = job.properties.get("total_progress", 100)
        progress = round(100 * current / total, 2) if total else 0.0
        click.echo(f"progress: {progress}%")

        # dask_dashboard_link
        click.echo(f"dask dashboard: {job.properties.get('dask_dashboard_link')}")

        # command
        click.echo(f"command: {job.properties.get('command')}")

        # output path
        click.echo(f"output path: {mapchete_config.get('output', {}).get('path')}")

        # bounds
        click.echo(f"bounds: {job.bounds}")

        # start time
        started = job.properties.get("started", "unknown")
        click.echo(f"started: {started}")

        # finish time
        finished = job.properties.get("finished", "unknown")
        click.echo(f"finished: {finished}")

        # runtime
        runtime = job.properties.get("runtime", "unknown")
        click.echo(f"runtime: {pretty_time(runtime) if runtime else None}")

        # last received update
        last_update = job.properties.get("updated", "unknown")
        click.echo(
            f"last received update: {pretty_time_passed(str_to_date(last_update))} ago"
        )

    if metadata_items:
        for i in metadata_items:
            click.echo(f"{i}: {job.properties.get(i)}")

    if verbose or metadata_items:
        # append newline
        click.echo("")
