import datetime
import json
import pytest
from shapely.geometry import shape
import time


def test_get_root(mhub_api):
    response = mhub_api.get("/")
    assert response.status_code == 200
    assert response.json()


def test_start_job(mhub_api, example_config_json):
    """Start a job and return job state."""
    job = mhub_api.start_job(**example_config_json)
    assert job.status_code == 201
    # running the TestClient sequentially actually results in a job state of "pending" for now
    assert job.state == "pending"


@pytest.mark.skip(reason="the background task does not run in the background in TestClient")
def test_cancel_job(mhub_api, example_config_json):
    """Cancel existing job."""
    job = mhub_api.start_job(**example_config_json)
    job = mhub_api.cancel_job(job.job_id)
    assert job.status_code == 200
    # running the TestClient sequentially actually results in a job state of "done" for now
    assert job.state == "cancelled"

def test_retry_job(mhub_api, example_config_json):
    """Retry a job and return job state."""
    job = mhub_api.start_job(**example_config_json)
    retried_job = mhub_api.retry_job(job.job_id)
    assert retried_job.status_code == 201


def test_job(mhub_api, example_config_json):
    """Return job metadata."""
    job = mhub_api.start_job(**example_config_json)
    job = mhub_api.job(job.job_id)
    assert job.status_code == 200
    assert job.state == "done"
    assert job.json
    assert isinstance(job.json, dict)


def test_job_state(mhub_api, example_config_json):
    """Return job state."""
    job = mhub_api.start_job(**example_config_json)
    assert mhub_api.job_state(job.job_id) == "done"


def test_jobs(mhub_api, example_config_json):
    """Return jobs metadata."""
    job = mhub_api.start_job(**example_config_json)
    job_id = job.job_id

    current = mhub_api.job(job_id)
    geom = shape(current)
    assert geom.is_valid
    assert not geom.is_empty

    # write another job
    another_job = job = mhub_api.start_job(**example_config_json)
    another_job_id = another_job.job_id
    current = mhub_api.job(another_job_id)
    geom = shape(current)
    assert geom.is_valid
    assert not geom.is_empty

    all_jobs = mhub_api.jobs()

    assert len(all_jobs) == 2

    # TODO: filter by time
    now = datetime.datetime.utcfromtimestamp(time.time())
    assert len(mhub_api.jobs(from_date=now)) == 0

    # filter by state
    assert len(mhub_api.jobs(state="done")) == 2
    assert len(mhub_api.jobs(state="failed")) == 0
    assert len(mhub_api.jobs(state="pending")) == 0

    # filter by command
    assert len(mhub_api.jobs(command="execute")) == 2

    # filter by bounds
    # '$geoIntersects' is a valid operation but it is not supported by Mongomock yet.
    with pytest.raises(NotImplementedError):
        assert len(mhub_api.jobs(bounds=[1, 2, 3, 4])) == 2
    with pytest.raises(NotImplementedError):
        assert len(mhub_api.jobs(bounds=[11, 12, 13, 14])) == 0

    # filter by job_name
    assert len(mhub_api.jobs(job_name="unnamed_job")) == 0
