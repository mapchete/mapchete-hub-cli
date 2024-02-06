import click
from tqdm import tqdm

from mapchete_hub_cli.cli import options
from mapchete_hub_cli.client import Client
from mapchete_hub_cli.exceptions import JobFailed


@click.command(
    short_help="Show job progress. Shorthand for mhub job <job_id> --progress"
)
@click.argument("job_id", type=click.STRING)
@click.option(
    "--interval",
    "-i",
    type=click.FLOAT,
    default=0.3,
    help="Request interval in seconds.",
)
@options.opt_debug
@click.pass_context
def progress(
    ctx,
    job_id,
    debug=False,
    interval=None,
):
    """
    Show job progress using a progress bar.

    JOB_ID can either be a valid job ID or :last:, in which case the CLI will automatically
    determine the most recently updated job.
    """
    try:
        client = Client(**ctx.obj)
        job = client.job(job_id)
        click.echo(f"job {job.job_id} {job.status}")
        show_progress(client, job_id, disable=debug, interval=interval)
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


def show_progress(client, job_id, disable=False, interval=0.3):
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
