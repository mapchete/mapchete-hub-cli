import json
import logging
from itertools import chain
from typing import List

import click

from mapchete_hub_cli import (
    COMMANDS,
    JOB_STATUSES,
    MHUB_CLI_ZONES_WAIT_TILES_COUNT,
    MHUB_CLI_ZONES_WAIT_TIME_SECONDS,
)
from mapchete_hub_cli.log import set_log_level
from mapchete_hub_cli.time import date_to_str, passed_time_to_timestamp, str_to_date


# click callbacks #
###################
def _set_debug_log_level(_, __, debug):
    if debug:  # pragma: no cover
        set_log_level(logging.DEBUG)
    return debug


def _check_dask_specs(_, __, dask_specs):
    if dask_specs:
        # read from JSON config
        with open(dask_specs, "r") as src:
            return json.loads(src.read())


def _get_timestamp(_, __, timestamp):
    """Convert timestamp to datetime object."""

    if timestamp:
        try:
            # for a convertable timestamp like '2019-11-01T15:00:00'
            timestamp = str_to_date(timestamp)
        except ValueError:
            try:
                timestamp = passed_time_to_timestamp(timestamp)
            except ValueError:
                raise click.BadParameter(
                    """either provide a timestamp like '2019-11-01T15:00:00' or a time """
                    """range in the format '1d', '12h', '30m', etc."""
                )
        return date_to_str(timestamp)


def _expand_str_list(_, __, str_list):
    if str_list:
        str_list = str_list.split(",")
    return str_list


def _validate_mapchete_files(_, __, mapchete_files) -> List[str]:
    if len(mapchete_files) == 0:
        raise click.MissingParameter("at least one mapchete file required")
    return mapchete_files


def _validate_zoom(_, __, zoom):
    if zoom:
        try:
            zoom_levels = list(map(int, zoom.split(",")))
        except ValueError:
            raise click.BadParameter("zoom levels must be integer values")
        try:
            if len(zoom_levels) > 2:
                raise ValueError("zooms can be maximum two items")
            for z in zoom_levels:
                if z < 0:
                    raise TypeError(f"zoom must be a positive integer: {zoom}")
            return zoom_levels
        except Exception as e:
            raise click.BadParameter(e)


# click arguments and options #
###############################
arg_mapchete_files = click.argument(
    "mapchete_files",
    type=click.Path(exists=True),
    nargs=-1,
    callback=_validate_mapchete_files,
)
opt_zoom = click.option(
    "--zoom",
    "-z",
    callback=_validate_zoom,
    help="Single zoom level or min and max separated by ','.",
)
opt_bounds = click.option(
    "--bounds",
    "-b",
    type=click.FLOAT,
    nargs=4,
    help="Left, bottom, right, top bounds in tile pyramid CRS.",
)
opt_bounds_crs = click.option(
    "--bounds-crs",
    type=click.STRING,
    help="CRS of --bounds.  [default: process CRS]",
)
opt_area = click.option(
    "--area",
    "-a",
    type=click.STRING,
    help="Process area as either WKT string or path to vector file.",
)
opt_area_crs = click.option(
    "--area-crs",
    type=click.STRING,
    help="CRS of --area (does not override CRS of vector file).  [default: process CRS]",
)
opt_point = click.option(
    "--point",
    "-p",
    type=click.FLOAT,
    nargs=2,
    help="Process tiles over single point location.",
)
opt_point_crs = click.option(
    "--point-crs", type=click.STRING, help="CRS of --point.  [default: process CRS]"
)
opt_tile = click.option(
    "--tile", "-t", type=click.INT, nargs=3, help="Zoom, row, column of single tile."
)
opt_overwrite = click.option(
    "--overwrite", "-o", is_flag=True, help="Overwrite if tile(s) already exist(s)."
)
opt_verbose = click.option(
    "--verbose", "-v", is_flag=True, help="Print info for each process tile."
)
opt_progress = click.option(
    "--progress", is_flag=True, help="Show progress in progress bar."
)
opt_debug = click.option(
    "--debug",
    "-d",
    is_flag=True,
    callback=_set_debug_log_level,
    help="Print debug log output.",
)
opt_job_name = click.option("--job-name", type=click.STRING, help="Name of job.")
opt_unique_by_job_name = click.option(
    "--unique-by-job-name",
    is_flag=True,
    help="Assume jobs with same job name are unique and only return latest job.",
)
opt_geojson = click.option("--geojson", "-g", is_flag=True, help="Print as GeoJSON.")
opt_output_path = click.option(
    "--output-path", "-p", type=click.STRING, help="Filter jobs by output_path."
)
opt_status = click.option(
    "--status",
    "-s",
    type=click.Choice(
        (
            [s.lower() for s in JOB_STATUSES.keys()]
            + [s.lower() for s in chain(*[g for g in JOB_STATUSES.values()])]
        )
    ),
    help="Filter jobs by job status.",
)
opt_command = click.option(
    "--command", "-c", type=click.Choice(COMMANDS), help="Filter jobs by command."
)
opt_dask_specs = click.option(
    "--dask-specs",
    "-w",
    type=click.STRING,
    callback=_check_dask_specs,
    help="Choose worker performance class.",
)
opt_dask_max_submitted_tasks = click.option(
    "--dask-max-submitted-tasks",
    type=click.INT,
    default=1000,
    help="Limit number of tasks being submitted to dask scheduler at once.",
    show_default=True,
)
opt_dask_chunksize = click.option(
    "--dask-chunksize",
    type=click.INT,
    default=100,
    help="Number tasks being submitted per request to dask scheduler at once.",
    show_default=True,
)
opt_dask_no_task_graph = click.option(
    "--dask-no-task-graph",
    is_flag=True,
    help="Don't compute task graph when using dask.",
)
opt_since = click.option(
    "--since",
    type=click.STRING,
    callback=_get_timestamp,
    help="Filter jobs by timestamp since given time.",
    default="7d",
    show_default=True,
)
opt_since_no_default = click.option(
    "--since",
    type=click.STRING,
    callback=_get_timestamp,
    help="Filter jobs by timestamp since given time.",
)
opt_until = click.option(
    "--until",
    type=click.STRING,
    callback=_get_timestamp,
    help="Filter jobs by timestamp until given time.",
)
opt_job_ids = click.option(
    "--job-ids",
    "-j",
    type=click.STRING,
    help="One or multiple job IDs separated by comma. If a job_id is ':last:', the CLI will automatically determine the most recently updated job.",
    callback=_expand_str_list,
)
opt_force = click.option("--force", "-f", is_flag=True, help="Don't ask, just do.")
opt_verbose = click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Print job details. (Does not work with --geojson.)",
)
opt_sort_by = click.option(
    "--sort-by",
    type=click.Choice(["started", "runtime", "status", "progress"]),
    default="status",
    help="Sort jobs.",
    show_default=True,
)
opt_metadata_items = click.option(
    "--metadata-items", "-i", type=click.STRING, callback=_expand_str_list
)
opt_make_zones = click.option(
    "--make-zones-on-zoom",
    "-zz",
    type=click.INT,
    help="Split up job into smaller jobs using a specified zoom level grid.",
)
opt_full_zones = click.option(
    "--full-zones",
    is_flag=True,
    help="Create full zones instead of intersection with bounds. (Only has effect with --make-zones-on-zoom.)",
)
opt_zones_wait_count = click.option(
    "--zones-wait-count",
    "-zwc",
    type=click.INT,
    default=MHUB_CLI_ZONES_WAIT_TILES_COUNT,
    help="Threshold for at how many submitted zones the mhub cli should wait, only triggers when --make-zones-on-zoom is used.",
    show_default=True,
)
opt_zones_wait_seconds = click.option(
    "--zones-wait-seconds",
    "-zws",
    type=click.INT,
    default=MHUB_CLI_ZONES_WAIT_TIME_SECONDS,
    help="How long should the mhub cli wait until submitting next zone in seconds, only triggers when --make-zones-on-zoom is used.",
    show_default=True,
)
opt_zone = click.option(
    "--zone",
    type=click.INT,
    nargs=3,
    help="Run on Zone defined by process pyramid grid.",
)
opt_use_old_image = click.option(
    "--use-old-image", is_flag=True, help="Force to rerun Job on image from first run."
)
opt_test_run = click.option(
    "--test-run",
    is_flag=True,
    help="Run a small scale test to validate mhub infrastructure runtime.",
)
