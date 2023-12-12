import json
import logging
from datetime import datetime, timedelta
from itertools import chain

import click
import oyaml as yaml
from tqdm import tqdm

from mapchete_hub_cli import (
    COMMANDS,
    DEFAULT_TIMEOUT,
    JOB_STATUSES,
    Client,
    __version__,
    load_mapchete_config,
)
from mapchete_hub_cli.exceptions import JobFailed
from mapchete_hub_cli.log import set_log_level

logger = logging.getLogger(__name__)

host_options = dict(host_ip="0.0.0.0", port=5000)


# click callbacks #
###################
def _set_debug_log_level(ctx, param, debug):
    if debug:  # pragma: no cover
        set_log_level(logging.DEBUG)
    return debug


def _check_dask_specs(ctx, param, dask_specs):
    if dask_specs:
        # read from JSON config
        with open(dask_specs, "r") as src:
            return json.loads(src.read())


def _get_timestamp(ctx, param, timestamp):
    """Convert timestamp to datetime object."""

    def str_to_date(date_str):
        """Convert string to datetime object."""
        if "T" in date_str:
            add_zulu = "Z" if date_str.endswith("Z") else ""
            try:
                return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f" + add_zulu)
            except ValueError:
                return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S" + add_zulu)
        else:
            return datetime(*map(int, date_str.split("-")))

    def date_to_str(date_obj, microseconds=True):
        """Return string from datetime object in the format."""
        return date_obj.strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ" if microseconds else "%Y-%m-%dT%H:%M:%SZ"
        )

    if timestamp:
        try:
            # for a convertable timestamp like '2019-11-01T15:00:00'
            timestamp = str_to_date(timestamp)
        except ValueError:
            # for a time range like '1d', '12h', '30m'
            try:
                time_types = {
                    "d": "days",
                    "h": "hours",
                    "m": "minutes",
                    "s": "seconds",
                }
                for k, v in time_types.items():
                    if timestamp.endswith(k):
                        timestamp = datetime.utcnow() - timedelta(
                            **{v: int(timestamp[:-1])}
                        )
                        break
                else:
                    raise ValueError()
            except ValueError:
                raise click.BadParameter(
                    """either provide a timestamp like '2019-11-01T15:00:00' or a time """
                    """range in the format '1d', '12h', '30m', etc."""
                )
        return date_to_str(timestamp)


def _expand_str_list(ctx, param, str_list):
    if str_list:
        str_list = str_list.split(",")
    return str_list


def _validate_mapchete_files(ctx, param, mapchete_files):
    if len(mapchete_files) == 0:
        raise click.MissingParameter("at least one mapchete file required")
    return mapchete_files


def _validate_zoom(ctx, param, zoom):
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


def _remote_versions_cb(ctx, param, value):
    if value:
        click.echo(Client().remote_version)
        ctx.exit()


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
    help="CRS of --bounds. (default: process CRS)",
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
    help="CRS of --area (does not override CRS of vector file). (default: process CRS)",
)
opt_point = click.option(
    "--point",
    "-p",
    type=click.FLOAT,
    nargs=2,
    help="Process tiles over single point location.",
)
opt_point_crs = click.option(
    "--point-crs", type=click.STRING, help="CRS of --point. (default: process CRS)"
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
)
opt_dask_chunksize = click.option(
    "--dask-chunksize",
    type=click.INT,
    default=100,
    help="Number tasks being submitted per request to dask scheduler at once.",
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
    help="Sort jobs. (default: status)",
)
opt_mhub_user = click.option(
    "--user",
    "-u",
    type=click.STRING,
    help="Username for basic auth. (Or set MHUB_USER env variable.)",
)
opt_mhub_password = click.option(
    "--password",
    "-p",
    type=click.STRING,
    help="Password for basic auth. (Or set MHUB_PASSWORD env variable.)",
)
opt_metadata_items = click.option(
    "--metadata-items", "-i", type=click.STRING, callback=_expand_str_list
)
opt_make_zones = click.option(
    "--make-zones-on-zoom",
    "-zz",
    type=click.INT,
    default=None,
    help="Split up job into smaller jobs using a specified zoom level grid.",
)
opt_zone = click.option(
    "--zone",
    type=click.INT,
    nargs=3,
    default=None,
    help="Run on Zone defined by process pyramid grid.",
)


@click.version_option(version=__version__, message="%(version)s")
@click.group(help="Process control on Mapchete Hub.")
@click.option(
    "--host",
    "-h",
    type=click.STRING,
    nargs=1,
    default=f"{host_options['host_ip']}:{host_options['port']}",
    help="""Address and port of mhub endpoint (default: """
    f"""{host_options['host_ip']}:{host_options['port']}). (Or set MHUB_HOST env variable.)""",
)
@click.option(
    "--timeout",
    type=click.INT,
    default=DEFAULT_TIMEOUT,
    help=f"Time in seconds to wait for server response. (default: {DEFAULT_TIMEOUT})",
)
@click.option(
    "--remote-versions",
    is_flag=True,
    callback=_remote_versions_cb,
    help="Show versions of installed packages on remote mapchete Hub.",
)
@opt_mhub_user
@opt_mhub_password
@click.pass_context
def mhub(ctx, host, **kwargs):
    """Main command group."""
    host = host if host.startswith("http") else f"http://{host}"
    host = host if host.endswith("/") else f"{host}/"
    ctx.obj = dict(host=host, **kwargs)


@mhub.command(short_help="Cancel jobs.")
@opt_job_ids
@opt_output_path
@opt_status
@opt_command
@opt_since_no_default
@opt_until
@opt_job_name
@opt_force
@opt_debug
@click.pass_context
def cancel(ctx, job_ids, debug=False, force=False, **kwargs):
    """Cancel jobs and their follow-up jobs if batch was submitted."""
    try:
        kwargs.update(from_date=kwargs.pop("since"), to_date=kwargs.pop("until"))

        if job_ids:
            jobs = [Client(**ctx.obj).job(job_id) for job_id in job_ids]

        else:
            if all([v is None for v in kwargs.values()]):  # pragma: no cover
                click.echo(ctx.get_help())
                raise click.UsageError(
                    "Please either provide one or more job IDs or other search values."
                )
            jobs = Client(**ctx.obj).jobs(**kwargs).values()

        def _yield_revokable_jobs(jobs):
            for j in jobs:
                if j.status in JOB_STATUSES["done"]:  # pragma: no cover
                    click.echo(f"Job {j.job_id} already in status {j.status}.")
                else:
                    yield j.job_id

        job_ids = list(_yield_revokable_jobs(jobs))

        if not job_ids:  # pragma: no cover
            click.echo("No revokable jobs found.")
            return

        for job_id in job_ids:
            click.echo(job_id)
        if force or click.confirm(
            f"Do you really want to cancel {len(job_ids)} job(s)?", abort=True
        ):
            for job_id in job_ids:
                job = Client(**ctx.obj).cancel_job(job_id)
                logger.debug(job.to_dict())
                click.echo(f"job {job.status}")

    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


@mhub.command(help="Execute a process.")
@arg_mapchete_files
@opt_zoom
@opt_area
@opt_area_crs
@opt_bounds
@opt_point
@opt_tile
@opt_overwrite
@opt_verbose
@opt_dask_specs
@opt_dask_max_submitted_tasks
@opt_dask_chunksize
@opt_dask_no_task_graph
@opt_debug
@opt_job_name
@opt_make_zones
@opt_zone
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
                tiles = (
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
                    job = Client(**ctx.obj).start_job(
                        command="execute",
                        config=mapchete_file,
                        params=dict(
                            kwargs,
                            bounds=bounds_intersection(bounds, tile.bounds())
                            if bounds
                            else tile.bounds(),
                            mode="overwrite" if overwrite else "continue",
                            dask_settings=dask_settings,
                            job_name=zone_job_name,
                        ),
                    )
                    click.echo(job.job_id)
            else:
                job = Client(**ctx.obj).start_job(
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
                    job = Client(**ctx.obj).job(job.job_id)
                    if job.properties.get("dask_dashboard_link"):
                        click.echo(
                            f"dask dashboard: {job.properties.get('dask_dashboard_link')}"
                        )
                    _show_progress(ctx, job.job_id, disable=debug)
                else:
                    click.echo(job.job_id)
        except Exception as e:  # pragma: no cover
            if debug:
                raise
            raise click.ClickException(e)


@mhub.command(short_help="Show job status.")
@click.argument("job_id", type=click.STRING)
@opt_geojson
@opt_metadata_items
@click.option("--traceback", is_flag=True, help="Print only traceback if available.")
@click.option("--show-config", is_flag=True, help="Print Mapchete config.")
@click.option("--show-params", is_flag=True, help="Print Mapchete parameters.")
@click.option("--show-process", is_flag=True, help="Print Mapchete process.")
@opt_progress
@opt_debug
@click.pass_context
def job(
    ctx,
    job_id,
    geojson=False,
    show_config=False,
    show_params=False,
    show_process=False,
    traceback=False,
    progress=False,
    debug=False,
    metadata_items=None,
    **kwargs,
):
    """
    Show job status.

    JOB_ID can either be a valid job ID or :last:, in which case the CLI will automatically
    determine the most recently updated job.
    """
    try:
        job = Client(**ctx.obj).job(job_id, geojson=geojson)
        if geojson:  # pragma: no cover
            click.echo(job)
            return
        elif show_config:
            click.echo(
                yaml.dump(job.to_dict()["properties"]["mapchete"]["config"], indent=2)
            )
            return
        elif show_params:
            for k, v in job.to_dict()["properties"]["mapchete"]["params"].items():
                if isinstance(v, list):
                    click.echo(f"{k}: {', '.join(map(str, v)) if v else None}")
                else:
                    click.echo(f"{k}: {v}")
            return
        elif show_process:
            process = job.to_dict()["properties"]["mapchete"]["config"].get("process")
            process = process if isinstance(process, list) else [process]
            for line in process:
                click.echo(line)
            return
        elif traceback:  # pragma: no cover
            click.echo(job.to_dict()["properties"].get("exception"))
            click.echo(job.to_dict()["properties"].get("traceback"))
        if progress:  # pragma: no cover
            click.echo(f"job {job.job_id} {job.status}")
            _show_progress(ctx, job_id, disable=debug)
        else:
            _print_job_details(job, metadata_items=metadata_items, verbose=True)
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


@mhub.command(
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
@opt_debug
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
        job = Client(**ctx.obj).job(job_id)
        click.echo(f"job {job.job_id} {job.status}")
        _show_progress(ctx, job_id, disable=debug, interval=interval)
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


@mhub.command(short_help="Show current jobs.")
@opt_output_path
@opt_status
@opt_command
@opt_since
@opt_until
@opt_job_name
@opt_sort_by
@opt_bounds
@opt_geojson
@opt_metadata_items
@opt_verbose
@opt_debug
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
        if geojson:
            click.echo(Client(**ctx.obj).jobs(geojson=True, **kwargs))
        else:
            # sort by status and then by timestamp
            jobs = _sort_jobs(
                Client(**ctx.obj).jobs(**kwargs).values(), sort_by=sort_by
            )
            logger.debug(jobs)
            if verbose:
                click.echo(f"{len(jobs)} jobs found. \n")
            for i in jobs:
                _print_job_details(i, metadata_items=metadata_items, verbose=verbose)
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


@mhub.command(short_help="Show available processes.")
@click.option(
    "--process_name", "-n", type=click.STRING, help="Print docstring of process."
)
@click.option("--docstrings", is_flag=True, help="Print docstrings of all processes.")
@opt_debug
@click.pass_context
def processes(ctx, process_name=None, docstrings=False, debug=None, **kwargs):
    """Show available processes."""

    def _print_process_info(process_module, docstrings=False):
        click.echo(
            click.style(process_module["title"], bold=docstrings, underline=docstrings)
        )
        if docstrings:
            click.echo(process_module["description"])

    try:
        res = Client(**ctx.obj).get("processes")
        if res.status_code != 200:  # pragma: no cover
            raise ConnectionError(res.json())

        # get all registered processes
        processes = {p.get("title"): p for p in res.json().get("processes")}

        # print selected process
        if process_name:
            _print_process_info(processes[process_name], docstrings=True)
        else:
            # print all processes
            click.echo(f"{len(processes)} processes found")
            for process_name in sorted(processes.keys()):
                _print_process_info(processes[process_name], docstrings=docstrings)
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


@mhub.command(short_help="Retry jobs.")
@opt_job_ids
@click.option(
    "--use-old-image", is_flag=True, help="Force to rerun Job on image from first run."
)
@opt_output_path
@opt_status
@opt_command
@opt_bounds
@opt_since_no_default
@opt_until
@opt_job_name
@opt_force
@opt_overwrite
@opt_debug
@opt_verbose
@opt_debug
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
        if job_ids:
            jobs = [Client(**ctx.obj).job(job_id) for job_id in job_ids]

        else:
            if all([v is None for v in kwargs.values()]):  # pragma: no cover
                click.echo(ctx.get_help())
                raise click.UsageError(
                    "Please either provide one or more job IDs or other search values."
                )
            jobs = Client(**ctx.obj).jobs(**kwargs).values()

        def _yield_retryable_jobs(jobs):
            for j in jobs:
                if j.status not in [
                    *JOB_STATUSES["done"],
                    "aborting",
                ]:  # pragma: no cover
                    click.echo(f"Job {j.job_id} still in status {j.status}.")
                else:
                    yield j.job_id

        job_ids = [j for j in _yield_retryable_jobs(jobs)]

        if not job_ids:  # pragma: no cover
            click.echo("No retryable jobs found.")
            return

        for job_id in job_ids:
            click.echo(job_id)
        if force or click.confirm(
            f"Do you really want to retry {len(job_ids)} job(s)?", abort=True
        ):
            for job_id in job_ids:
                job = Client(**ctx.obj).retry_job(job_id, use_old_image=use_old_image)
                click.echo(f"job {job.job_id} {job.status}")
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


# helper fucntions #
####################
def _print_job_details(job, metadata_items=None, verbose=False):
    def _pretty_runtime(elapsed):
        minutes, seconds = divmod(elapsed, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:  # pragma: no cover
            return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
        elif minutes:  # pragma: no cover
            return f"{int(minutes)}m {int(seconds)}s"
        else:
            return f"{round(seconds, 3)}s"

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
        click.echo(f"runtime: {_pretty_runtime(runtime) if runtime else None}")

        # last received update
        last_update = job.properties.get("updated", "unknown")
        click.echo(f"last received update: {last_update}")

    if metadata_items:
        for i in metadata_items:
            click.echo(f"{i}: {job.properties.get(i)}")

    if verbose or metadata_items:
        # append newline
        click.echo("")


def _show_progress(ctx, job_id, disable=False, interval=0.3):
    try:
        progress_iter = (
            Client(**ctx.obj).job(job_id).progress(smooth=True, interval=interval)
        )
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
