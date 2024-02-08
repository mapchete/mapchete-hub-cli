import logging

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
        stalled_jobs = Client(**ctx.obj).stalled_jobs(
            inactive_since=inactive_since,
            pending_since=pending_since,
            check_inactive_dashboard=not skip_dashboard_check,
            msg_writer=click.echo,
        )
        if stalled_jobs:  # pragma: no cover
            click.echo(f"found {len(stalled_jobs)} potentially stalled jobs:")
            for job in stalled_jobs:
                click.echo(job.job_id)
            if force or click.confirm(
                f"Do you really want to cancel {'and retry ' if retry else ''}{len(stalled_jobs)} job(s)?",
                abort=True,
            ):
                if retry:
                    stalled_jobs.cancel_and_retry(
                        msg_writer=click.echo, use_old_image=use_old_image
                    )
                else:
                    stalled_jobs.cancel(msg_writer=click.echo)
        else:
            click.echo("No stalled jobs found.")

    except Exception as exc:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(str(exc))
