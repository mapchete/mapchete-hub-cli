import json
import click
from datetime import datetime, timedelta
from itertools import chain
import logging
from tqdm import tqdm

from mapchete_hub_cli import API, commands, default_timeout, job_states, __version__
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


def _check_worker_specs(ctx, param, worker_specs):
    res = API(**ctx.obj).get("worker_specs")
    if res.status_code != 200:  # pragma: no cover
        raise ConnectionError(res.json())
    for w in res.json().keys():
        if worker_specs not in res.json().keys():
            raise TypeError(f"Worker specs name not in {res.json().keys()}!!!")


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
            return datetime(*map(int, date_str.split('-')))


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


def _expand_job_ids(ctx, param, job_ids):
    if job_ids:
        job_ids = job_ids.split(",")
    return job_ids


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
    "--debug", "-d",
    is_flag=True,
    callback=_set_debug_log_level,
    help="Print debug log output."
)
opt_job_name = click.option(
    "--job-name",
    type=click.STRING,
    help="Name of job."
)
opt_geojson = click.option(
    "--geojson", "-g",
    is_flag=True,
    help="Print as GeoJSON."
)
opt_output_path = click.option(
    "--output-path", "-p",
    type=click.STRING,
    help="Filter jobs by output_path."
)
opt_state = click.option(
    "--state", "-s",
    type=click.Choice(
        (
            [s.lower() for s in job_states.keys()] +
            [s.lower() for s in chain(*[g for g in job_states.values()])]
        )
    ),
    help="Filter jobs by job state."
)
opt_command = click.option(
    "--command", "-c",
    type=click.Choice(commands),
    help="Filter jobs by command."
)
opt_worker_specs = click.option(
    "--worker_specs", "-w",
    type=click.STRING,
    callback=_check_worker_specs,
    default="default",
    help="Choose worker performance class."
)
opt_since = click.option(
    "--since",
    type=click.STRING,
    callback=_get_timestamp,
    help="Filter jobs by timestamp since given time.",
    default="7d"
)
opt_since_no_default = click.option(
    "--since",
    type=click.STRING,
    callback=_get_timestamp,
    help="Filter jobs by timestamp since given time."
)
opt_until = click.option(
    "--until",
    type=click.STRING,
    callback=_get_timestamp,
    help="Filter jobs by timestamp until given time.",
)
opt_job_ids = click.option(
    "--job-ids", "-j",
    type=click.STRING,
    help="One or multiple job IDs separated by comma.",
    callback=_expand_job_ids
)
opt_force = click.option(
    "--force", "-f",
    is_flag=True,
    help="Don't ask, just do."
)
opt_verbose = click.option(
    "--verbose", "-v",
    is_flag=True,
    help="Print job details. (Does not work with --geojson.)"
)
opt_sort_by = click.option(
    "--sort-by",
    type=click.Choice(["started", "runtime", "state", "progress"]),
    default="state",
    help="Sort jobs. (default: state)"
)
opt_mhub_user = click.option(
    "--user", "-u",
    type=click.STRING,
    help="Username for basic auth."
)
opt_mhub_password = click.option(
    "--password", "-p",
    type=click.STRING,
    help="Password for basic auth."
)

@click.version_option(version=__version__, message="%(version)s")
@click.group(help="Process control on Mapchete Hub.")
@click.option(
    "--host", "-h",
    type=click.STRING,
    nargs=1,
    default=f"{host_options['host_ip']}:{host_options['port']}",
    help="""Address and port of mhub endpoint (default: """
        f"""{host_options['host_ip']}:{host_options['port']})."""
)
@click.option(
    "--timeout",
    type=click.INT,
    default=default_timeout,
    help=f"Time in seconds to wait for server response. (default: {default_timeout})"
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
@opt_state
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

        kwargs.update(
            from_date=kwargs.pop("since"),
            to_date=kwargs.pop("until")
        )

        if job_ids:
            jobs = [API(**ctx.obj).job(job_id) for job_id in job_ids]

        else:
            if all([v is None for v in kwargs.values()]):  # pragma: no cover
                click.echo(ctx.get_help())
                raise click.UsageError(
                    "Please either provide one or more job IDs or other search values."
                )
            jobs = API(**ctx.obj).jobs(**kwargs).values()

        def _yield_revokable_jobs(jobs):
            for j in jobs:
                if j.state in job_states["done"]:  # pragma: no cover
                    click.echo(f"Job {j.job_id} already in state {j.state}.")
                else:
                    yield j.job_id

        job_ids = [j for j in _yield_revokable_jobs(jobs)]

        if not job_ids:  # pragma: no cover
            click.echo("No revokable jobs found.")
            return

        for job_id in job_ids:
            click.echo(job_id)
        if force or click.confirm(f"Do you really want to cancel {len(job_ids)} job(s)?", abort=True):
            for job_id in job_ids:
                job = API(**ctx.obj).cancel_job(job_id)
                logger.debug(job.json)
                click.echo(f"job {job.state}")

    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


@mhub.command(help="Execute a process.")
@arg_mapchete_files
@opt_zoom
@opt_bounds
@opt_point
@opt_tile
@opt_overwrite
@opt_verbose
@opt_worker_specs
@opt_debug
@opt_job_name
@click.pass_context
def execute(
    ctx,
    mapchete_files,
    overwrite=False,
    verbose=False,
    debug=False,
    **kwargs
):
    """Execute a process."""
    for mapchete_file in mapchete_files:
        try:
            job = API(**ctx.obj).start_job(
                command="execute",
                config=mapchete_file,
                params=dict(
                    kwargs,
                    mode="overwrite" if overwrite else "continue",
                )
            )
            if verbose:  # pragma: no cover
                click.echo(f"job {job.job_id} {job.state}")
                _show_progress(ctx, job.job_id, disable=debug)
            else:
                click.echo(job.job_id)
        except Exception as e:  # pragma: no cover
            raise
            raise click.ClickException(e)


@mhub.command(short_help="Show job status.")
@click.argument("job_id", type=click.STRING)
@opt_geojson
@click.option(
    "--traceback",
    is_flag=True,
    help="Print only traceback if available."
)
@opt_progress
@opt_debug
@click.pass_context
def job(ctx, job_id, geojson=False, traceback=False, progress=False, debug=False, **kwargs):
    """Show job status."""
    try:
        job = API(**ctx.obj).job(job_id, geojson=geojson)
        if geojson:  # pragma: no cover
            click.echo(job)
        elif traceback:  # pragma: no cover
            click.echo(job.json["properties"].get("traceback"))
        if progress:  # pragma: no cover
            click.echo(f"job {job.job_id} {job.state}")
            _show_progress(ctx, job_id, disable=debug)
        else:
            _print_job_details(job, verbose=True)
    except Exception as e:  # pragma: no cover
        raise click.ClickException(e)


@mhub.command(short_help="Show current jobs.")
@opt_output_path
@opt_state
@opt_command
@opt_since
@opt_until
@opt_job_name
@opt_sort_by
@opt_bounds
@opt_geojson
@opt_verbose
@opt_debug
@click.pass_context
def jobs(
     ctx,
    geojson=False,
    verbose=False,
    sort_by=None,
    debug=False,
    **kwargs
):
    """Show current jobs."""

    def _sort_jobs(jobs, sort_by=None):
        if sort_by == "state":
            return list(
                sorted(
                    jobs,
                    key=lambda x: (
                        x.json["properties"]["state"],
                        x.json["properties"]["updated"]
                    )
                )
            )
        elif sort_by in ["started", "runtime"]:
            return list(
                sorted(jobs, key=lambda x: x.json["properties"][sort_by] or 0.)
            )
        elif sort_by == "progress":
            def _get_progress(job):
                properties = job.json.get("properties", {})
                current = properties.get("current_progress")
                total = properties.get("total_progress")
                return 100 * current / total if total else 0.
            return list(sorted(jobs, key=lambda x: _get_progress(x)))

    kwargs.update(from_date=kwargs.pop("since"), to_date=kwargs.pop("until"))
    try:
        if geojson:
            click.echo(
                API(**ctx.obj).jobs(geojson=True, **kwargs)
            )
        else:
            # sort by state and then by timestamp
            jobs = _sort_jobs(API(**ctx.obj).jobs(**kwargs).values(), sort_by=sort_by)
            logger.debug(jobs)
            if verbose:
                click.echo(f"{len(jobs)} jobs found. \n")
            for i in jobs:
                _print_job_details(i, verbose=verbose)
    except Exception as e:  # pragma: no cover
        raise
        raise click.ClickException(e)


@mhub.command(short_help="Show available processes.")
@click.option(
    "--process_name", "-n", type=click.STRING, help="Print docstring of process."
)
@click.option(
    "--docstrings", is_flag=True, help="Print docstrings of all processes."
)
@opt_debug
@click.pass_context
def processes(ctx, process_name=None, docstrings=False, **kwargs):
    """Show available processes."""
    def _print_process_info(process_module, docstrings=False):
        click.echo(
            click.style(
                process_module["title"],
                bold=docstrings,
                underline=docstrings
            )
        )
        if docstrings:
            click.echo(process_module["description"])

    try:
        res = API(**ctx.obj).get("processes")
        if res.status_code != 200:  # pragma: no cover
            raise ConnectionError(res.json())

        # get all registered processes
        processes = {
            p.get("title"): p
            for p in res.json().get("processes")
        }

        # print selected process
        if process_name:
            _print_process_info(processes[process_name], docstrings=True)
        else:
            # print all processes
            click.echo(f"{len(processes)} processes found")
            for process_name in sorted(processes.keys()):
                _print_process_info(processes[process_name], docstrings=docstrings)
    except Exception as e:  # pragma: no cover
        raise click.ClickException(e)


@mhub.command(short_help="Show available worker specs.")
@opt_debug
@click.pass_context
def worker_specs(ctx, **kwargs):
    res = API(**ctx.obj).get("worker_specs")
    if res.status_code != 200:  # pragma: no cover
        raise ConnectionError(res.json())
    click.echo(json.dumps(res.json(), indent=4, sort_keys=True))


@mhub.command(short_help="Retry jobs.")
@opt_job_ids
@opt_output_path
@opt_state
@opt_command
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
    overwrite=False,
    verbose=False,
    force=False,
    debug=False,
    **kwargs
):
    """Retry jobs and their follow-up jobs if batch was submitted."""
    kwargs.update(
        from_date=kwargs.pop("since"),
        to_date=kwargs.pop("until")
    )

    try:
        if job_ids:
            jobs = [API(**ctx.obj).job(job_id) for job_id in job_ids]

        else:
            if all([v is None for v in kwargs.values()]):  # pragma: no cover
                click.echo(ctx.get_help())
                raise click.UsageError(
                    "Please either provide one or more job IDs or other search values."
                )
            jobs = API(**ctx.obj).jobs(**kwargs).values()

        def _yield_retryable_jobs(jobs):
            for j in jobs:
                if j.state not in job_states["done"]:  # pragma: no cover
                    click.echo(f"Job {j.job_id} still in state {j.state}.")
                else:
                    yield j.job_id

        job_ids = [j for j in _yield_retryable_jobs(jobs)]

        if not job_ids:  # pragma: no cover
            click.echo("No retryable jobs found.")
            return

        for job_id in job_ids:
            click.echo(job_id)
        if force or click.confirm(f"Do you really want to retry {len(job_ids)} job(s)?", abort=True):
            for job_id in job_ids:
                job = API(**ctx.obj).retry_job(job_id)
                click.echo(f"job {job.state}")
    except Exception as e:  # pragma: no cover
        if debug:
            raise
        raise click.ClickException(e)


# helper fucntions #
####################
def _print_job_details(job, verbose=False):

    def _pretty_runtime(elapsed):
        minutes, seconds = divmod(elapsed, 60)
        hours, minutes = divmod(minutes, 60)
        if hours:  # pragma: no cover
            return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
        elif minutes:  # pragma: no cover
            return f"{int(minutes)}m {int(seconds)}s"
        else:
            return f"{round(seconds, 3)}s"

    for group, states in job_states.items():  # pragma: no cover
        for state in states:
            if job.state == state:
                if group == "todo":
                    color = "blue"
                elif group == "doing":
                    color = "yellow"
                elif state == "done":
                    color = "green"
                elif state == "failed":
                    color = "red"
                elif state in ["aborting", "cancelled"]:
                    color = "magenta"
    properties = job.json["properties"]
    mapchete_config = properties.get("mapchete", {}).get("config", {})

    # job ID and job state
    click.echo(click.style(f"{job.job_id}", fg=color, bold=True))

    if verbose:
        # job name
        click.echo(f"job name: {properties.get('job_name')}")

        # state
        click.echo(click.style(f"state: {job.state}"))

        # progress
        current = properties.get("current_progress")
        total = properties.get("total_progress")
        progress = round(100 * current / total, 2) if total else 0.
        click.echo(f"progress: {progress}%")

        # command
        click.echo(f"command: {properties.get('command')}")

        # output path
        click.echo(f"output path: {mapchete_config.get('output', {}).get('path')}")

        # bounds
        # try:
        #     bounds = ", ".join(map(str, shape(job).bounds))
        # except:  # pragma: no cover
        #     bounds = None
        # click.echo(f"bounds: {bounds}")

        # start time
        started = properties.get("started", "unknown")
        click.echo(f"started: {started}")

        # finish time
        finished = properties.get("finished", "unknown")
        click.echo(f"finished: {finished}")

        # runtime
        runtime = properties.get("runtime", "unknown")
        click.echo(f"runtime: {_pretty_runtime(runtime) if runtime else None}")

        # last received update
        last_update = properties.get("updated", "unknown")
        click.echo(f"last received update: {last_update}")

        # append newline
        click.echo("")


def _show_progress(ctx, job_id, disable=False):
    try:
        progress = API(**ctx.obj).job(job_id).progress()
        click.echo("wait for job progress...")
        i = next(progress)
        last_progress = 0
        with tqdm(total=i["total_progress"], disable=disable) as pbar:
            for i in progress:
                current_progress = i["current_progress"]
                pbar.update(current_progress - last_progress)
                last_progress = current_progress
        click.echo(f"job {i['state']}")
    except JobFailed as e:  # pragma: no cover
        click.echo(f"Job {job_id} failed: {e}")
        return
    except Exception as e:  # pragma: no cover
        raise click.ClickException(e)
