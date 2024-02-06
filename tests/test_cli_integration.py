import os
from uuid import uuid4

import pytest
import requests

from mapchete_hub_cli.enums import Status

TEST_ENDPOINT = os.environ.get("MHUB_HOST", "http://0.0.0.0:5000")


def _endpoint_available():
    try:
        response = requests.get(TEST_ENDPOINT)
        return response.status_code == 200
    except requests.exceptions.ConnectionError:
        return False


ENDPOINT_AVAILABLE = _endpoint_available()

todo_or_doing = [
    Status.pending,
    Status.running,
    Status.parsing,
    Status.initializing,
]


def test_cli(cli):
    result = cli.run("--help")
    assert result.exit_code == 0


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_remote_versions(cli):
    result = cli.run("--remote-versions")
    assert result.exit_code == 0
    assert "20" in result.output


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_execute(mhub_integration_client, cli, example_config_mapchete):
    result = cli.run(f"execute {example_config_mapchete.path}")
    try:
        assert result.exit_code == 0
    except AssertionError:
        print(result.output.strip())
        raise
    job_id = result.output.strip()

    job = mhub_integration_client.job(job_id)
    job.wait(wait_for_max=120)
    assert mhub_integration_client.job(job_id).status == Status.done


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_execute_dask_spec_json(
    mhub_integration_client, cli, example_config_mapchete, custom_dask_specs_json
):
    result = cli.run(
        f"execute {example_config_mapchete.path} --dask-specs {custom_dask_specs_json} --tile 8 0 0"
    )
    assert result.exit_code == 0
    job_id = result.output.strip()

    job = mhub_integration_client.job(job_id)
    assert job.properties["dask_specs"].get("worker_threads") == 2


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_execute_progress(mhub_integration_client, cli, example_config_mapchete):
    job_name = uuid4().hex
    result = cli.run(
        f"execute {example_config_mapchete.path} --verbose --job-name {job_name}"
    )
    assert result.exit_code == 0

    jobs = mhub_integration_client.jobs(job_name=job_name)
    assert len(jobs) == 1
    for job in jobs:
        assert job.status == Status.done


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_execute_errors(cli, example_config_mapchete):
    result = cli.run("execute")
    assert result.exit_code == 2
    assert "at least one mapchete file required" in result.output

    result = cli.run(f"execute {example_config_mapchete.path} --zoom foo")
    assert result.exit_code == 2
    assert "zoom levels must be integer values" in result.output

    result = cli.run(f"execute {example_config_mapchete.path} --zoom 1,2,3")
    assert result.exit_code == 2
    assert "zooms can be maximum two items" in result.output

    result = cli.run(f"execute {example_config_mapchete.path} --zoom -1")
    assert result.exit_code == 2
    assert "zoom must be a positive integer" in result.output


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_progress(mhub_integration_client, cli, example_config_mapchete):
    job_name = uuid4().hex
    result = cli.run(f"execute {example_config_mapchete.path} --job-name {job_name}")
    assert result.exit_code == 0
    job_id = result.output.strip()

    result = cli.run(f"progress {job_id}")
    print(result.output)
    assert result.exit_code == 0


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_cancel_by_job_id(mhub_integration_client, cli, example_config_mapchete):
    # execute job
    result = cli.run(f"execute {example_config_mapchete.path}")
    assert result.exit_code == 0
    job_id = result.output.strip()

    # cancel job
    result = cli.run(f"cancel -j {job_id} -f")
    assert result.exit_code == 0

    # wait and make sure it is cancelled
    job = mhub_integration_client.job(job_id)
    job.wait(wait_for_max=120, raise_exc=False)
    assert mhub_integration_client.job(job_id).status == Status.cancelled


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_cancel_by_search(mhub_integration_client, cli, example_config_mapchete):
    # execute job
    result = cli.run(f"execute {example_config_mapchete.path}")
    assert result.exit_code == 0
    job_id = result.output.strip()

    # cancel job
    result = cli.run("cancel --since 1m -f")
    print(result.output)
    assert result.exit_code == 0

    # wait and make sure it is cancelled
    job = mhub_integration_client.job(job_id)
    job.wait(wait_for_max=120, raise_exc=False)
    assert mhub_integration_client.job(job_id).status == Status.cancelled


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_job(mhub_integration_client, cli, example_config_mapchete):
    # execute job
    result = cli.run(f"execute {example_config_mapchete.path}")
    assert result.exit_code == 0
    job_id = result.output.strip()

    # get job information
    result = cli.run(f"job {job_id}")
    assert result.exit_code == 0
    assert "started" in result.output

    # get job information as geojson
    result = cli.run(f"job {job_id} --geojson")
    assert result.exit_code == 0
    assert "Feature" in result.output

    # print job config
    result = cli.run(f"job {job_id} --show-config")
    assert result.exit_code == 0
    assert "output" in result.output

    # print job params
    result = cli.run(f"job {job_id} --show-params")
    assert result.exit_code == 0
    assert "mode" in result.output

    # print job process
    result = cli.run(f"job {job_id} --show-process")
    assert result.exit_code == 0
    assert "mapchete.processes.convert" in result.output


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_jobs(mhub_integration_client, cli, example_config_mapchete):
    # spawn two jobs
    jobs = [
        mhub_integration_client.start_job(
            command="execute", config=example_config_mapchete.path
        ).job_id
        for _ in range(2)
    ]

    result = cli.run("jobs")
    assert result.exit_code == 0
    for job_id in jobs:
        assert job_id in result.output

    result = cli.run("jobs --since 1m")
    assert result.exit_code == 0
    filtered = result.output.splitlines()
    for job_id in jobs:
        assert job_id in filtered

    result = cli.run("jobs --since 2019-11-01T15:00:00")
    assert result.exit_code == 0
    filtered = result.output.splitlines()
    for job_id in jobs:
        assert job_id in filtered

    result = cli.run("jobs --since 2019-11-01T15:00:00.000")
    assert result.exit_code == 0
    filtered = result.output.splitlines()
    for job_id in jobs:
        assert job_id in filtered

    result = cli.run("jobs --until 1h")
    assert result.exit_code == 0
    filtered = result.output.splitlines()
    for job_id in jobs:
        assert job_id not in filtered


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_jobs_sort_by(cli):
    result = cli.run("jobs --sort-by status")
    assert result.exit_code == 0

    result = cli.run("jobs --sort-by progress")
    assert result.exit_code == 0

    result = cli.run("jobs --sort-by runtime")
    assert result.exit_code == 0


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_jobs_print(cli):
    result = cli.run("jobs --verbose")
    assert result.exit_code == 0

    result = cli.run("jobs --geojson")
    assert result.exit_code == 0

    result = cli.run("jobs -i runtime")
    assert result.exit_code == 0


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_jobs_errors(mhub_integration_client, cli, example_config_mapchete):
    result = cli.run("jobs --until foo")
    assert result.exit_code == 2
    assert "either provide a timestamp like" in result.output


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_processes_list(cli):
    result = cli.run("processes")
    assert result.exit_code == 0
    assert "mapchete.processes.examples.example_process" in result.output


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_processes_list_docstrings(cli):
    result = cli.run("processes --docstrings")
    assert result.exit_code == 0
    assert "Example process for testing." in result.output


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_processes_single(cli):
    result = cli.run("processes -n mapchete.processes.examples.example_process")
    assert result.exit_code == 0
    assert "Example process for testing." in result.output


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_retry_by_job_id(mhub_integration_client, cli, example_config_mapchete):
    # execute job
    result = cli.run(f"execute {example_config_mapchete.path} --zoom 2")
    assert result.exit_code == 0
    job_id = result.output.strip()

    # wait and make sure it is finished
    job = mhub_integration_client.job(job_id)
    job.wait(wait_for_max=120, raise_exc=False)
    assert mhub_integration_client.job(job_id).status == Status.done

    # retry job
    result = cli.run(f"retry -j {job_id} -f")
    assert result.exit_code == 0


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_retry_by_search(mhub_integration_client, cli, example_config_mapchete):
    # execute job
    result = cli.run(f"execute {example_config_mapchete.path} --zoom 2")
    assert result.exit_code == 0
    job_id = result.output.strip()

    # wait and make sure it is finished
    job = mhub_integration_client.job(job_id)
    job.wait(wait_for_max=120, raise_exc=False)
    assert mhub_integration_client.job(job_id).status == Status.done

    # retry job
    result = cli.run("retry --since 5s -f")
    assert result.exit_code == 0
