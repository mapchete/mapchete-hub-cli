import logging
from typing import Generator, List, Union

import click

from mapchete_hub_cli.cli import options
from mapchete_hub_cli.client import JOB_STATUSES, Client, Job, Jobs

logger = logging.getLogger(__name__)


@click.command(short_help="Cancel jobs.")
@options.opt_job_ids
@options.opt_output_path
@options.opt_status
@options.opt_command
@options.opt_since_no_default
@options.opt_until
@options.opt_job_name
@options.opt_force
@options.opt_debug
@click.pass_context
def cancel(ctx, job_ids, debug=False, force=False, **kwargs):
    """Cancel jobs and their follow-up jobs if batch was submitted."""
    client = Client(**ctx.obj)
    try:
        kwargs.update(from_date=kwargs.pop("since"), to_date=kwargs.pop("until"))

        if job_ids:
            jobs = [client.job(job_id) for job_id in job_ids]

        else:
            if all([v is None for v in kwargs.values()]):  # pragma: no cover
                click.echo(ctx.get_help())
                raise click.UsageError(
                    "Please either provide one or more job IDs or other search values."
                )
            jobs = client.jobs(**kwargs)

        jobs = list(yield_revokable_jobs(jobs))

        if not jobs:  # pragma: no cover
            click.echo("No revokable jobs found.")
            return

        for job in jobs:
            click.echo(job.job_id)
        if force or click.confirm(
            f"Do you really want to cancel {len(jobs)} job(s)?", abort=True
        ):
            for job in jobs:
                job.cancel()
                logger.debug(job.to_dict())
                click.echo(f"job {job.status}")

    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


def yield_revokable_jobs(jobs: Union[Jobs, List[Job]]) -> Generator[Job, None, None]:
    for job in jobs:
        if job.status in JOB_STATUSES["done"]:  # pragma: no cover
            click.echo(f"Job {job.job_id} already in status {job.status}.")
        else:
            yield job
