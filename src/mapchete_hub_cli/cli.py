import logging

import click
import oyaml as yaml
import requests
from tqdm import tqdm

from mapchete_hub_cli import (
    DEFAULT_TIMEOUT,
    JOB_STATUSES,
    Client,
    __version__,
    load_mapchete_config,
)
from mapchete_hub_cli.cli import options
from mapchete_hub_cli.enums import Status
from mapchete_hub_cli.exceptions import JobFailed
from mapchete_hub_cli.time import (
    date_to_str,
    passed_time_to_timestamp,
    pretty_time,
    pretty_time_passed,
)

logger = logging.getLogger(__name__)

# helper fucntions #
####################


def _print_job_details(job, metadata_items=None, verbose=False):
    color = "white"
    for group, statuss in JOB_STATUSES.items():  # pragma: no cover
        for status in statuss:
            if job.status == status:
                if group == "todo":
                    color = "blue"
                elif group == "doing":
                    color = "yellow"
                elif status == "done":
                    color = "green"
                elif status == "failed":
                    color = "red"
                elif status in ["aborting", "cancelled"]:
                    color = "magenta"
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
        current = job.properties.get("current_progress")
        total = job.properties.get("total_progress")
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
        click.echo(f"last received update: {last_update}")

    if metadata_items:
        for i in metadata_items:
            click.echo(f"{i}: {job.properties.get(i)}")

    if verbose or metadata_items:
        # append newline
        click.echo("")


def _show_progress(client, job_id, disable=False, interval=0.3):
    try:
        progress_iter = client.job(job_id).progress(smooth=True, interval=interval)
        click.echo("wait for job progress...")
        i = next(progress_iter)
        last_progress = i["current_progress"]
        with tqdm(
            total=i["total_progress"],
            initial=last_progress,
            disable=disable,
            unit="task",
        ) as pbar:
            for i in progress_iter:
                current_progress = i["current_progress"]
                pbar.update(current_progress - last_progress)
                last_progress = current_progress
        click.echo(f"job {i['status']}")
    except JobFailed as e:  # pragma: no cover
        click.echo(f"Job {job_id} failed: {e}")
        return


def bounds_intersection(bounds1, bounds2):
    return (
        max([bounds1[0], bounds2[0]]),
        max([bounds1[1], bounds2[1]]),
        min([bounds1[2], bounds2[2]]),
        min([bounds1[3], bounds2[3]]),
    )
