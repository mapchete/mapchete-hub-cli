import click

from mapchete_hub_cli.cli import options
from mapchete_hub_cli.client import Client

MHUB_TEST_BUCKET_KEY = "s3://eox-mhub-cache/mhub_test/"

MAPCHETE_TEST_CONFIG = {
    "process": "mapchete.processes.convert",
    "input": {
        "inp": "https://ungarj.github.io/mapchete_testdata/tiled_data/raster/cleantopo/"
    },
    "output": {
        "format": "GTiff",
        "bands": 1,
        "dtype": "uint16",
        "path": MHUB_TEST_BUCKET_KEY,
    },
    "pyramid": {"grid": "geodetic", "metatiling": 2},
    "zoom_levels": {"min": 0, "max": 13},
    "bounds": [0, 1, 2, 3],
    "dask_specs": {
        "worker_cores": 0.2,
        "worker_cores_limit": 0.3,
        "worker_memory": 1.0,
        "worker_memory_limit": 2.0,
        "worker_threads": 1,
        "scheduler_cores": 1,
        "scheduler_cores_limit": 1.0,
        "scheduler_memory": 1.0,
        "adapt_options": {"minimum": 0, "maximum": 2, "active": "true"},
    },
}


@click.command(help="Run a minor test to verify mhub infractucture runtime.")
@options.opt_dask_specs
@options.opt_dask_max_submitted_tasks
@options.opt_dask_chunksize
@options.opt_dask_no_task_graph
@click.pass_context
def test_run(
    ctx,
    dask_no_task_graph=False,
    dask_max_submitted_tasks=1000,
    dask_chunksize=100,
    **kwargs
):
    """Small test build-in CLI."""
    dask_settings = dict(
        process_graph=not dask_no_task_graph,
        max_submitted_tasks=dask_max_submitted_tasks,
        chunksize=dask_chunksize,
    )
    client = Client(**ctx.obj)

    client.start_job(
        command="execute",
        config=MAPCHETE_TEST_CONFIG,
        params=dict(
            kwargs,
            bounds=MAPCHETE_TEST_CONFIG["bounds"],
            mode="overwrite",
            dask_settings=dask_settings,
            job_name="mhub_cli_test_run",
        ),
    )
