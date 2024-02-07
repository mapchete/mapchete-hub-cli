from time import sleep

import click

from mapchete_hub_cli.cli import options
from mapchete_hub_cli.cli.progress import show_progress_bar
from mapchete_hub_cli.client import Client
from mapchete_hub_cli.parser import load_mapchete_config


@click.command(help="Execute a process.")
@options.arg_mapchete_files
@options.opt_zoom
@options.opt_area
@options.opt_area_crs
@options.opt_bounds
@options.opt_point
@options.opt_tile
@options.opt_overwrite
@options.opt_verbose
@options.opt_dask_specs
@options.opt_dask_max_submitted_tasks
@options.opt_dask_chunksize
@options.opt_dask_no_task_graph
@options.opt_debug
@options.opt_job_name
@options.opt_make_zones
@options.opt_full_zones
@options.opt_zones_wait_count
@options.opt_zones_wait_seconds
@options.opt_zone
@click.pass_context
def execute(
    ctx,
    mapchete_files,
    bounds=None,
    overwrite=False,
    verbose=False,
    debug=False,
    dask_no_task_graph=False,
    dask_max_submitted_tasks=1000,
    dask_chunksize=100,
    make_zones_on_zoom=None,
    full_zones=False,
    zones_wait_count=5,
    zones_wait_seconds=10,
    job_name=None,
    zone=None,
    **kwargs,
):
    """Execute a process."""
    dask_settings = dict(
        process_graph=not dask_no_task_graph,
        max_submitted_tasks=dask_max_submitted_tasks,
        chunksize=dask_chunksize,
    )
    client = Client(**ctx.obj)
    for mapchete_file in mapchete_files:
        try:
            if make_zones_on_zoom is not None and bounds is None:
                raise click.UsageError("--make-zones-on-zoom requires --bounds")
            elif make_zones_on_zoom is not None or zone is not None:
                try:
                    from tilematrix import TilePyramid
                except ImportError:  # pragma: no cover
                    raise ImportError(
                        "please install mapchete_hub_cli[zones] extra for this feature."
                    )
                config = load_mapchete_config(mapchete_file)
                tp = TilePyramid(config["pyramid"]["grid"])
                tiles = list(
                    tp.tiles_from_bounds(bounds, make_zones_on_zoom)
                    if make_zones_on_zoom
                    else [tp.tile(*zone)]
                )
                for tile in tiles:
                    zone_job_name = (
                        f"{job_name}-{tile.zoom}-{tile.row}-{tile.col}"
                        if job_name
                        else None
                    )
                    process_bounds = (
                        bounds_intersection(bounds, tile.bounds())
                        if (bounds and not full_zones)
                        else tile.bounds()
                    )
                    if len(tiles) >= zones_wait_count:
                        sleep(zones_wait_seconds)
                    job = client.start_job(
                        command="execute",
                        config=mapchete_file,
                        params=dict(
                            kwargs,
                            bounds=process_bounds,
                            mode="overwrite" if overwrite else "continue",
                            dask_settings=dask_settings,
                            job_name=zone_job_name,
                        ),
                    )
                    click.echo(job.job_id)
            else:
                job = client.start_job(
                    command="execute",
                    config=mapchete_file,
                    params=dict(
                        kwargs,
                        bounds=bounds,
                        mode="overwrite" if overwrite else "continue",
                        dask_settings=dask_settings,
                        job_name=job_name,
                    ),
                )
                if verbose:  # pragma: no cover
                    click.echo(f"job {job.job_id} {job.status}")
                    if job.properties.get("dask_dashboard_link"):
                        click.echo(
                            f"dask dashboard: {job.properties.get('dask_dashboard_link')}"
                        )
                    show_progress_bar(job, disable=debug)
                else:
                    click.echo(job.job_id)
        except Exception as e:  # pragma: no cover
            if debug:
                raise
            raise click.ClickException(e)


def bounds_intersection(bounds1, bounds2):
    return (
        max([bounds1[0], bounds2[0]]),
        max([bounds1[1], bounds2[1]]),
        min([bounds1[2], bounds2[2]]),
        min([bounds1[3], bounds2[3]]),
    )
