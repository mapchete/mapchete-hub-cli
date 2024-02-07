import logging
from typing import Set

import click
import requests

from mapchete_hub_cli.cli import options
from mapchete_hub_cli.client import Client, Job
from mapchete_hub_cli.enums import Status
from mapchete_hub_cli.time import (
    date_to_str,
    passed_time_to_timestamp,
    pretty_time_passed,
)

logger = logging.getLogger(__name__)


@click.command(short_help="Abort stalled jobs.")
@click.pass_context
@click.option(
    "--inactive-since",
    type=click.STRING,
    default="5h",
    help="Time since jobs have been inactive.",
    show_default=True,
)
@click.option(
    "--pending-since",
    type=click.STRING,
    default="3d",
    help="Time since jobs have been pending.",
    show_default=True,
)
@click.option(
    "--skip-dashboard-check", is_flag=True, help="Skip dashboard availability check."
)
@click.option("--retry", is_flag=True, help="Retry instead of cancel stalled jobs.")
@options.opt_use_old_image
@options.opt_force
@options.opt_debug
def clean(
    ctx: click.Context,
    inactive_since: str = "5h",
    pending_since: str = "3d",
    skip_dashboard_check: bool = False,
    retry: bool = False,
    use_old_image: bool = False,
    force: bool = False,
    debug: bool = False,
):
    """
    Checks for probably stalled jobs and offers to cancel or retry them.

    The check looks at three properties:\n
    - jobs which are pending for too long\n
    - jobs which are parsing|initializing|running but have been inactive for too long\n
    - jobs which are running, have a scheduler but scheduler dashboard is not available\n
    """
    try:
        client = Client(**ctx.obj)
        jobs = stalled_jobs(
            client=client,
            inactive_since=inactive_since,
            pending_since=pending_since,
            check_inactive_dashboard=not skip_dashboard_check,
        )
        if jobs:
            click.echo(f"found {len(jobs)} potentially stalled jobs:")
            for job in jobs:
                click.echo(job.job_id)
            if force or click.confirm(
                f"Do you really want to cancel {'and retry ' if retry else ''}{len(jobs)} job(s)?",
                abort=True,
            ):
                for job in jobs:
                    job.cancel()
                    logger.debug(job.to_dict())
                    if retry:
                        retried_job = job.retry(use_old_image=use_old_image)
                        click.echo(
                            f"job {job.job_id} {job.status} and retried as {retried_job.job_id} ({retried_job.status})"
                        )
                    else:
                        click.echo(f"job {job} {job.status}")

        else:
            click.echo("No stalled jobs found.")

    except Exception as exc:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(str(exc))


def stalled_jobs(
    client: Client,
    inactive_since: str = "5h",
    pending_since: str = "3d",
    check_inactive_dashboard: bool = True,
) -> Set[Job]:
    stalled = set()

    # jobs which have been pending for too long
    for job in client.jobs(
        status=Status.pending,
        to_date=date_to_str(passed_time_to_timestamp(pending_since)),
    ):
        logger.debug(
            "job %s %s state since %s", job.job_id, job.status, job.last_updated
        )
        click.echo(
            f"{job.job_id} {job.status} since {pretty_time_passed(job.last_updated)}"
        )
        stalled.add(job)

    # jobs which have been inactive for too long
    for status in [Status.parsing, Status.initializing, Status.running]:
        for job in client.jobs(
            status=status,
            to_date=date_to_str(passed_time_to_timestamp(inactive_since)),
        ):
            logger.debug(
                "job %s %s but has been inactive since %s",
                job.job_id,
                job.status,
                job.last_updated,
            )
            click.echo(
                f"{job.job_id} {job.status} but has been inactive since {pretty_time_passed(job.last_updated)}"
            )
            stalled.add(job)

    # running jobs with unavailable dashboard
    if check_inactive_dashboard:
        for job in client.jobs(status=Status.running):
            dashboard_link = job.properties.get("dask_dashboard_link")
            # NOTE: jobs can be running without haveing a dashboard
            if dashboard_link:
                status_code = requests.get(dashboard_link).status_code
                if status_code != 200:
                    logger.debug(
                        "job %s %s but dashboard %s returned status code %s",
                        job.job_id,
                        job.status,
                        dashboard_link,
                        status_code,
                    )
                    click.echo(
                        f"{job.job_id} {job.status} but has inactive dashboard (status code {status_code}, job inactive since {pretty_time_passed(job.last_updated)})"
                    )
                    stalled.add(job)

    return stalled
