import click

from mapchete_hub_cli.cli import options
from mapchete_hub_cli.client import JOB_STATUSES, Client


@click.command(short_help="Retry jobs.")
@options.opt_job_ids
@options.opt_use_old_image
@options.opt_output_path
@options.opt_status
@options.opt_command
@options.opt_bounds
@options.opt_since_no_default
@options.opt_until
@options.opt_job_name
@options.opt_force
@options.opt_overwrite
@options.opt_debug
@options.opt_verbose
@options.opt_debug
@click.pass_context
def retry(
    ctx,
    job_ids=None,
    use_old_image=False,
    overwrite=False,
    verbose=False,
    force=False,
    debug=False,
    **kwargs,
):
    """Retry jobs and their follow-up jobs if batch was submitted."""
    kwargs.update(from_date=kwargs.pop("since"), to_date=kwargs.pop("until"))

    try:
        client = Client(**ctx.obj)
        if job_ids:
            jobs = [client.job(job_id) for job_id in job_ids]

        else:
            if all([v is None for v in kwargs.values()]):  # pragma: no cover
                click.echo(ctx.get_help())
                raise click.UsageError(
                    "Please either provide one or more job IDs or other search values."
                )
            jobs = client.jobs(**kwargs)

        def _yield_retryable_jobs(jobs):
            for job in jobs:
                if job.status not in [
                    *JOB_STATUSES["done"],
                    "aborting",
                ]:  # pragma: no cover
                    click.echo(f"Job {job.job_id} still in status {job.status}.")
                else:
                    yield job

        jobs = [j for j in _yield_retryable_jobs(jobs)]

        if not jobs:  # pragma: no cover
            click.echo("No retryable jobs found.")
            return

        for job in jobs:
            click.echo(job.job_id)
        if force or click.confirm(
            f"Do you really want to retry {len(jobs)} job(s)?", abort=True
        ):
            for job in jobs:
                retried_job = job.retry(use_old_image=use_old_image)
                click.echo(
                    f"job {job.job_id} {job.status} and retried as {retried_job.job_id} ({retried_job.status})"
                )
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)
