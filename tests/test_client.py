import datetime
import json
import os
import time

import pytest
from shapely.geometry import shape

from mapchete_hub_cli import exceptions


def test_get_root(mhub_client):
    response = mhub_client.get("/")
    assert response.status_code == 200
    assert response.json()


def test_remote_version(mhub_client):
    assert mhub_client.remote_version


def test_start_job(mhub_client, example_config_json):
    """Start a job and return job state."""
    job = mhub_client.start_job(**example_config_json)
    assert job.status_code == 201
    job.wait(wait_for_max=120)
    assert mhub_client.job(job.job_id).state == "done"

    assert isinstance(job.to_json(), str)


def test_start_job_custom_process(mhub_client, example_config_custom_process_json):
    """Start a job and return job state."""
    job = mhub_client.start_job(**example_config_custom_process_json)
    assert job.status_code == 201
    job.wait(wait_for_max=120)
    assert mhub_client.job(job.job_id).state == "done"


def test_start_job_python_process(mhub_client, example_config_python_process_json):
    """Start a job and return job state."""
    job = mhub_client.start_job(
        **example_config_python_process_json,
        basedir=os.path.dirname(os.path.realpath(__file__))
    )
    assert job.status_code == 201
    job.wait(wait_for_max=120)
    assert mhub_client.job(job.job_id).state == "done"


def test_start_job_failing_process(mhub_client, example_config_process_exception_json):
    """Start a job and return job state."""
    job = mhub_client.start_job(**example_config_process_exception_json)
    assert job.status_code == 201
    with pytest.raises(exceptions.JobFailed):
        job.wait(wait_for_max=120)
    assert mhub_client.job(job.job_id).state == "failed"


@pytest.mark.skip(
    reason="the background task does not run in the background in TestClient"
)
def test_cancel_job(mhub_client, example_config_json):
    """Cancel existing job."""
    job = mhub_client.start_job(**example_config_json)
    job = mhub_client.cancel_job(job.job_id)
    assert job.status_code == 200
    # running the TestClient sequentially actually results in a job state of "done" for now
    assert mhub_client.job(job.job_id).state == "failed"


def test_retry_job(mhub_client, example_config_json):
    """Retry a job and return job state."""
    job = mhub_client.start_job(**example_config_json)
    retried_job = mhub_client.retry_job(job.job_id)
    assert retried_job.status_code == 201


def test_retry_last_job(mhub_client, example_config_json):
    """Retry a job and return job state."""
    mhub_client.start_job(**example_config_json)
    retried_job = mhub_client.retry_job(":last:")
    assert retried_job.status_code == 201


def test_job(mhub_client, example_config_json):
    """Return job metadata."""
    job = mhub_client.start_job(**example_config_json)
    job = mhub_client.job(job.job_id)
    assert job.status_code == 200
    assert job.state == "done"
    assert job.to_dict()
    assert isinstance(job.to_dict(), dict)


def test_job_state(mhub_client, example_config_json):
    """Return job state."""
    job = mhub_client.start_job(**example_config_json)
    assert mhub_client.job_state(job.job_id) == "done"


def test_last_job_state(mhub_client, example_config_json):
    """Return job state."""
    mhub_client.start_job(**example_config_json)
    assert mhub_client.job_state(":last:") == "done"


def test_job_states(mhub_client, example_config_json):
    """Return job state."""
    mhub_client.start_job(**example_config_json)
    states = mhub_client.jobs_states()
    assert isinstance(states, dict)
    for job_id, state in states.items():
        assert job_id
        assert state


def test_list_jobs_bounds(mhub_client, example_config_json):
    job_id = mhub_client.start_job(
        **dict(example_config_json, params=dict(example_config_json["params"], zoom=2))
    ).job_id

    jobs = mhub_client.jobs(bounds=[0, 1, 2, 3])
    assert job_id in jobs

    jobs = mhub_client.jobs(bounds=[10, 1, 12, 3])
    assert job_id not in jobs


def test_list_jobs_output_path(mhub_client, example_config_json):
    job_id = mhub_client.start_job(
        **dict(example_config_json, params=dict(example_config_json["params"], zoom=2))
    ).job_id

    jobs = mhub_client.jobs(output_path=example_config_json["config"]["output"]["path"])
    assert job_id in jobs

    jobs = mhub_client.jobs(output_path="foo")
    assert job_id not in jobs


def test_list_jobs_state(mhub_client, example_config_json):
    job_id = mhub_client.start_job(
        **dict(example_config_json, params=dict(example_config_json["params"], zoom=2))
    ).job_id

    jobs = mhub_client.jobs(state="done")
    assert job_id in jobs

    jobs = mhub_client.jobs(state="cancelled")
    assert job_id not in jobs


def test_list_jobs_job_name(mhub_client, example_config_json):
    job_id = mhub_client.start_job(
        **dict(
            example_config_json,
            params=dict(example_config_json["params"], zoom=2, job_name="foo"),
        )
    ).job_id

    jobs = mhub_client.jobs(job_name="foo")
    assert job_id in jobs

    jobs = mhub_client.jobs(job_name="bar")
    assert job_id not in jobs


def test_list_jobs_from_date(mhub_client, example_config_json):
    job_id = mhub_client.start_job(
        **dict(
            example_config_json,
            params=dict(example_config_json["params"], zoom=2, job_name="foo"),
        )
    ).job_id

    now = datetime.datetime.utcfromtimestamp(time.time()).strftime("%Y-%m-%dT%H:%M:%SZ")
    jobs = mhub_client.jobs(from_date=now)
    assert job_id in jobs

    future = datetime.datetime.utcfromtimestamp(time.time() + 60).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    jobs = mhub_client.jobs(from_date=future)
    assert job_id not in jobs


def test_list_jobs_to_date(mhub_client, example_config_json):
    job_id = mhub_client.start_job(
        **dict(
            example_config_json,
            params=dict(example_config_json["params"], zoom=2, job_name="foo"),
        )
    ).job_id

    now = datetime.datetime.utcfromtimestamp(time.time() + 60).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    jobs = mhub_client.jobs(to_date=now)
    assert job_id in jobs

    past = datetime.datetime.utcfromtimestamp(time.time() - 60).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    jobs = mhub_client.jobs(to_date=past)
    assert job_id not in jobs


def test_geojson_output(mhub_client, example_config_json):
    job = mhub_client.start_job(
        **dict(
            example_config_json,
            params=dict(example_config_json["params"], zoom=2, job_name="foo"),
        )
    )
    job.wait(wait_for_max=120)
    geojson = mhub_client.job(job.job_id, geojson=True)
    feature = json.loads(geojson)
    assert shape(feature["geometry"]).is_valid
    for i in ["id", "properties", "type"]:
        assert i in feature

    jobs = mhub_client.jobs(geojson=True)
    for feature in json.loads(jobs)["features"]:
        assert shape(feature["geometry"]).is_valid
        for i in ["id", "properties", "type"]:
            assert i in feature


def test_errors(mhub_client, example_config_json):
    # start job with invalid command
    with pytest.raises(ValueError):
        mhub_client.start_job(**dict(example_config_json, command="foo"))

    # job rejected
    with pytest.raises(exceptions.JobRejected):
        mhub_client.start_job(
            **dict(
                example_config_json,
                params="bar",
            )
        )

    # job not found
    with pytest.raises(exceptions.JobNotFound):
        mhub_client.job("foo")
    with pytest.raises(exceptions.JobNotFound):
        mhub_client.cancel_job("foo")
