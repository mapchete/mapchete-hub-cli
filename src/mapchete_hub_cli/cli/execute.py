from time import sleep
from typing import List, Optional, Tuple

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
@options.opt_zones_within_area
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
@options.opt_force
@click.pass_context
def execute(
    ctx: click.Context,
    mapchete_files: List[str],
    bounds: Optional[Tuple[float, float, float, float]] = None,
    overwrite: bool = False,
    verbose: bool = False,
    debug: bool = False,
    dask_no_task_graph: bool = False,
    dask_max_submitted_tasks: int = 1000,
    dask_chunksize: int = 100,
    make_zones_on_zoom: Optional[int] = None,
    full_zones: int = False,
    zones_wait_count: int = 100,
    zones_wait_seconds: float = 1.0,
    job_name: Optional[str] = None,
    zone: Optional[str] = None,
    force: bool = False,
    area: Optional[str] = None,
    zones_within_area: bool = False,
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
            if make_zones_on_zoom is not None and (
                bounds is None and area is None
            ):  # pragma: no cover
                raise click.UsageError(
                    "--make-zones-on-zoom requires --bounds and/or --area"
                )
            elif make_zones_on_zoom is not None or zone is not None:
                try:
                    from mapchete.config.parse import guess_geometry
                    from mapchete.tile import BufferedTilePyramid, BufferedTile
                except ImportError:  # pragma: no cover
                    raise ImportError(
                        "please install mapchete_hub_cli[zones] extra for this feature."
                    )
                tp = BufferedTilePyramid(
                    load_mapchete_config(mapchete_file)["pyramid"]["grid"]
                )
                zones: List[BufferedTile] = []
                if zone:
                    zones = [tp.tile(*zone)]
                else:
                    if bounds:
                        zones = [
                            tile_zone
                            for tile_zone in tp.tiles_from_bounds(
                                bounds, make_zones_on_zoom
                            )
                        ]
                    if area:
                        geometry, crs = guess_geometry(area)
                        if crs != tp.crs:  # pragma: no cover
                            raise ValueError(
                                f"area CRS ({crs}) must be the same as BufferedTilePyramid CRS ({tp.crs})"
                            )
                        zones = zones or [
                            tile_zone
                            for tile_zone in tp.tiles_from_geom(
                                geometry, make_zones_on_zoom
                            )
                        ]

                        if zones_within_area:
                            zones = [
                                tile_zone
                                for tile_zone in zones
                                if tile_zone.bbox.within(geometry)
                            ]
                        else:
                            zones = [
                                tile_zone
                                for tile_zone in zones
                                if tile_zone.bbox.intersects(geometry)
                            ]
                if (
                    force
                    or len(zones) == 1
                    or click.confirm(
                        f"Do you really want to submit {len(zones)} jobs?", abort=True
                    )
                ):
                    for tile_zone in zones:
                        zone_job_name = (
                            f"{job_name}-{tile_zone.zoom}-{tile_zone.row}-{tile_zone.col}"
                            if job_name
                            else None
                        )
                        process_bounds = (
                            bounds_intersection(bounds, tile_zone.bounds)
                            if (bounds and not full_zones)
                            else tile_zone.bounds
                        )
                        if len(zones) >= zones_wait_count:  # pragma: no cover
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
