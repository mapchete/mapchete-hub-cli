import datetime
import json
import os
import pytest
import requests
from shapely.geometry import shape
import time

from mapchete_hub_cli.exceptions import JobAborting, JobCancelled

TEST_ENDPOINT = os.environ.get("MHUB_HOST", "http://0.0.0.0:5000")


def _endpoint_available():
    try:
        response = requests.get(TEST_ENDPOINT)
        return response.status_code == 200
    except requests.exceptions.ConnectionError:
        return False


ENDPOINT_AVAILABLE = _endpoint_available()


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_start_job(mhub_integration_client, example_config_json):
    """Start a job and return job state."""
    job = mhub_integration_client.start_job(**example_config_json)
    assert job.status_code == 201
    # running the TestClient sequentially actually results in a job state of "pending" for now
    assert job.state == "pending"


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_cancel_job(mhub_integration_client, example_config_json):
    """Cancel existing job."""
    job = mhub_integration_client.start_job(**example_config_json)
    job = mhub_integration_client.cancel_job(job.job_id)
    assert job.status_code == 200
    with pytest.raises((JobAborting, JobCancelled)):
        job.wait(wait_for_max=120)
    job = mhub_integration_client.job(job.job_id)
    assert job.state in ["cancelled", "aborting"]


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_retry_job(mhub_integration_client, example_config_json):
    """Retry a job and return job state."""
    job = mhub_integration_client.start_job(**example_config_json)
    retried_job = mhub_integration_client.retry_job(job.job_id)
    assert retried_job.status_code == 201


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_job(mhub_integration_client, example_config_json):
    """Return job metadata."""
    job = mhub_integration_client.start_job(**example_config_json)
    job = mhub_integration_client.job(job.job_id)
    assert job.status_code == 200
    assert job.state == "running"
    assert job.to_dict()
    assert isinstance(job.to_dict(), dict)


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_job_state(mhub_integration_client, example_config_json):
    """Return job state."""
    job = mhub_integration_client.start_job(**example_config_json)
    assert mhub_integration_client.job_state(job.job_id) in ["running", "pending"]


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_list_jobs_bounds(mhub_integration_client, example_config_json):
    job_id = mhub_integration_client.start_job(
        **dict(example_config_json, params=dict(example_config_json["params"], zoom=1))
    ).job_id

    jobs = mhub_integration_client.jobs(bounds=[0, 1, 2, 3])
    assert job_id in jobs

    jobs = mhub_integration_client.jobs(bounds=[10, 1, 12, 3])
    assert job_id not in jobs


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_list_jobs_output_path(mhub_integration_client, example_config_json):
    job_id = mhub_integration_client.start_job(
        **dict(example_config_json, params=dict(example_config_json["params"], zoom=1))
    ).job_id

    jobs = mhub_integration_client.jobs(
        output_path=example_config_json["config"]["output"]["path"]
    )
    assert job_id in jobs

    jobs = mhub_integration_client.jobs(output_path="foo")
    assert job_id not in jobs


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_list_jobs_state(mhub_integration_client, example_config_json):
    job = mhub_integration_client.start_job(
        **dict(example_config_json, params=dict(example_config_json["params"], zoom=1))
    )
    job_id = job.job_id

    job.wait()

    jobs = mhub_integration_client.jobs(state="done")
    assert job_id in jobs

    jobs = mhub_integration_client.jobs(state="cancelled")
    assert job_id not in jobs


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_list_jobs_job_name(mhub_integration_client, example_config_json):
    job_id = mhub_integration_client.start_job(
        **dict(
            example_config_json,
            params=dict(example_config_json["params"], zoom=1, job_name="foo"),
        )
    ).job_id

    jobs = mhub_integration_client.jobs(job_name="foo")
    assert job_id in jobs

    jobs = mhub_integration_client.jobs(job_name="bar")
    assert job_id not in jobs


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_list_jobs_from_date(mhub_integration_client, example_config_json):
    job_id = mhub_integration_client.start_job(
        **dict(
            example_config_json,
            params=dict(example_config_json["params"], zoom=1, job_name="foo"),
        )
    ).job_id

    now = datetime.datetime.utcfromtimestamp(time.time()).strftime("%Y-%m-%dT%H:%M:%SZ")
    jobs = mhub_integration_client.jobs(from_date=now)
    assert job_id in jobs

    future = datetime.datetime.utcfromtimestamp(time.time() + 60).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    jobs = mhub_integration_client.jobs(from_date=future)
    assert job_id not in jobs


@pytest.mark.skipif(
    not ENDPOINT_AVAILABLE,
    reason="requires up and running endpoint using docker-compose",
)
def test_list_jobs_to_date(mhub_integration_client, example_config_json):
    job_id = mhub_integration_client.start_job(
        **dict(
            example_config_json,
            params=dict(example_config_json["params"], zoom=1, job_name="foo"),
        )
    ).job_id

    now = datetime.datetime.utcfromtimestamp(time.time() + 60).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    jobs = mhub_integration_client.jobs(to_date=now)
    assert job_id in jobs

    past = datetime.datetime.utcfromtimestamp(time.time() - 60).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    jobs = mhub_integration_client.jobs(to_date=past)
    assert job_id not in jobs
