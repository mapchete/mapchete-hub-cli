import logging

import click

from mapchete_hub_cli.cli import options
from mapchete_hub_cli.cli.job import print_job_details
from mapchete_hub_cli.client import Client

logger = logging.getLogger(__name__)


@click.command(short_help="Show current jobs.")
@options.opt_output_path
@options.opt_status
@options.opt_command
@options.opt_since
@options.opt_until
@options.opt_job_name
@options.opt_sort_by
@options.opt_bounds
@options.opt_geojson
@options.opt_metadata_items
@options.opt_verbose
@options.opt_debug
@click.pass_context
def jobs(
    ctx,
    geojson=False,
    verbose=False,
    sort_by=None,
    debug=False,
    metadata_items=None,
    **kwargs,
):
    """Show current jobs."""

    def _sort_jobs(jobs, sort_by=None):
        if sort_by == "status":
            return list(
                sorted(
                    jobs,
                    key=lambda x: (
                        x.to_dict()["properties"]["status"],
                        x.to_dict()["properties"]["updated"],
                    ),
                )
            )
        elif sort_by in ["started", "runtime"]:
            return list(
                sorted(jobs, key=lambda x: x.to_dict()["properties"][sort_by] or 0.0)
            )
        elif sort_by == "progress":

            def _get_progress(job):
                properties = job.to_dict().get("properties", {})
                current = properties.get("current_progress")
                total = properties.get("total_progress")
                return 100 * current / total if total else 0.0

            return list(sorted(jobs, key=lambda x: _get_progress(x)))

    kwargs.update(from_date=kwargs.pop("since"), to_date=kwargs.pop("until"))
    try:
        client = Client(**ctx.obj)
        if geojson:
            click.echo(client.jobs(geojson=True, **kwargs))
        else:
            # sort by status and then by timestamp
            jobs = _sort_jobs(client.jobs(**kwargs), sort_by=sort_by)
            logger.debug(jobs)
            if verbose:
                click.echo(f"{len(jobs)} jobs found. \n")
            for i in jobs:
                print_job_details(i, metadata_items=metadata_items, verbose=verbose)
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)
