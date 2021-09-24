"""
Convenience tools to communicate with mapchete Hub REST API.

This module wraps around the requests module for real-life usage and FastAPI's TestClient()
in order to be able to test mhub CLI.
"""

from collections import namedtuple, OrderedDict
import datetime
import json
import logging
import os
import py_compile
import requests
from requests.exceptions import ConnectionError, HTTPError
import time
import uuid
import oyaml as yaml

from mapchete_hub_cli.exceptions import (
    JobAborting,
    JobCancelled,
    JobFailed,
    JobNotFound,
    JobRejected,
)


logger = logging.getLogger(__name__)


default_timeout = 5
job_states = {
    "todo": ["pending"],
    "doing": ["running", "aborting"],
    "done": ["done", "failed", "cancelled"],
}
commands = ["execute"]


class Job:
    """Job metadata class."""

    def __init__(
        self, status_code=None, state=None, job_id=None, json=None, _client=None
    ):
        """Initialize."""
        self.status_code = status_code
        self.state = state
        self.job_id = job_id
        self.exists = True if status_code == 409 else False
        self._dict = OrderedDict(json.items())
        self.__geo_interface__ = self._dict["geometry"]
        self._client = _client

    def to_dict(self):
        return self._dict

    def to_json(self, indent=4):
        return json.dumps(self._dict, indent=indent)

    def __repr__(self):  # pragma: no cover
        """Print Job."""
        return f"Job(status_code={self.status_code}, state={self.state}, job_id={self.job_id}, dict={self.to_dict()}"

    def wait(self, wait_for_max=None, raise_exc=True):
        """Block until job has finished processing."""
        list(self.progress(wait_for_max=wait_for_max, raise_exc=raise_exc))

    def progress(self, wait_for_max=None, raise_exc=True, interval=0.3, smooth=False):
        """Yield job progress messages."""
        progress_iter = self._client.job_progress(
            self.job_id,
            wait_for_max=wait_for_max,
            raise_exc=raise_exc,
            interval=interval,
        )
        if smooth:
            i = next(progress_iter)
            last_progress = i["current_progress"]
            yield i
            for i in progress_iter:
                current_progress = i["current_progress"]
                jump = current_progress - last_progress
                for j in range(jump):
                    time.sleep(interval / jump)
                    yield dict(
                        i,
                        current_progress=last_progress + j,
                    )
                last_progress = current_progress
        else:
            yield from progress_iter


class Client:
    """Client class which abstracts REST interface."""

    def __init__(
        self,
        host="localhost:5000",
        timeout=None,
        user=None,
        password=None,
        _test_client=None,
        **kwargs,
    ):
        """Initialize."""
        env_host = os.environ.get("MHUB_HOST")
        if env_host:  # pragma: no cover
            logger.debug(f"got mhub host from env: {env_host}")
            host = env_host
        host = host if host.startswith("http") else f"http://{host}"
        host = host if host.endswith("/") else f"{host}/"
        self.host = host if host.endswith("/") else f"{host}/"
        logger.debug(f"use host name {self.host}")
        self.timeout = timeout or default_timeout
        self._user = user or os.environ.get("MHUB_USER")
        self._password = password or os.environ.get("MHUB_PASSWORD")
        self._test_client = _test_client
        self._client = _test_client if _test_client else requests
        self._baseurl = "" if _test_client else host

    def _request(self, request_type, url, **kwargs):
        _request_func = {
            "GET": self._client.get,
            "POST": self._client.post,
            "PUT": self._client.put,
            "DELETE": self._client.delete,
        }
        if request_type not in _request_func:  # pragma: no cover
            raise ValueError(f"unknown request type '{request_type}'")
        try:
            request_url = self._baseurl + url
            request_kwargs = self._get_kwargs(kwargs)
            logger.debug(f"{request_type}: {request_url}, {request_kwargs}")
            res = _request_func[request_type](request_url, **request_kwargs)
            logger.debug(f"response: {res}")
            if res.status_code == 401:
                raise HTTPError("Authorization failure")
            return res
        except ConnectionError:  # pragma: no cover
            raise ConnectionError(f"no mhub server found at {self.host}")

    def get(self, url, **kwargs):
        """Make a GET request to _test_client or host."""
        return self._request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        """Make a POST request to _test_client or host."""
        return self._request("POST", url, **kwargs)

    def delete(self, url, **kwargs):
        """Make a DELETE request to _test_client or host."""
        return self._request("DELETE", url, **kwargs)

    def start_job(self, command="execute", config=None, params=None, basedir=None):
        """
        Start a job and return job state.

        Sends HTTP POST to /jobs/<job_id> and appends mapchete configuration as well
        as processing parameters as JSON.

        Parameters
        ----------
        config : path or dict
            Either path to .mapchete file or dictionary with mapchete parameters.
        command : str
            Either "execute" or "index".
        params : dict
            Mapchete execution parameters, e.g.

            bounds : list
                Left, bottom, right, top coordinate of process area.
            point : list
                X and y coordinate of point over process tile.
            tile : list
                Zoom, row and column of process tile.
            geometry : str
                GeoJSON representaion of process area.
            dask_specs: str
                One of EOX Mhub worker spec names choose from: [
                    default|s2_16bit_regular|s2_16bit_large|s1_large|custom
                ]
            zoom : list or int
                Minimum and maximum zoom level or single zoom level.

        Returns
        -------
        mapchete_hub.api.Job
        """
        basedir = basedir or os.getcwd()
        job = OrderedDict(
            command=command,
            config=load_mapchete_config(config, basedir),
            params=params or {},
        )

        # make sure correct command is provided
        if command not in commands:  # pragma: no cover
            raise ValueError(f"invalid command given: {command}")

        logger.debug(f"send job to API")
        res = self.post(
            f"processes/{command}/execution", data=json.dumps(job), timeout=self.timeout
        )

        if res.status_code != 201:
            raise JobRejected(res.json())
        else:
            job_id = res.json()["id"]
            logger.debug(f"job {job_id} sent")
            return Job(
                status_code=res.status_code,
                state=res.json()["properties"]["state"],
                job_id=job_id,
                json=res.json(),
                _client=self,
            )

    def cancel_job(self, job_id):
        """Cancel existing job."""
        res = self.delete(f"jobs/{job_id}", timeout=self.timeout)
        if res.status_code == 404:
            raise JobNotFound(f"job {job_id} does not exist")
        return Job(
            status_code=res.status_code,
            state=self.job_state(job_id),
            job_id=job_id,
            json=res.json(),
            _client=self,
        )

    def retry_job(self, job_id):
        """
        Retry a job and its children and return job state.

        Sends HTTP POST to /jobs/<job_id> and appends mapchete configuration as well
        as processing parameters as JSON.

        Returns
        -------
        mapchete_hub.api.Job
        """
        existing_job = self.job(job_id)
        return self.start_job(
            config=existing_job.to_dict()["properties"]["mapchete"]["config"],
            command=existing_job.to_dict()["properties"]["mapchete"]["command"],
            params=existing_job.to_dict()["properties"]["mapchete"]["params"],
        )

    def job(self, job_id, geojson=False, indent=4):
        """Return job metadata."""
        res = self.get(f"jobs/{job_id}", timeout=self.timeout)
        if res.status_code == 404:
            raise JobNotFound(f"job {job_id} does not exist")
        else:
            return (
                json.dumps(res.json(), indent=indent)
                if geojson
                else Job(
                    status_code=res.status_code,
                    state=res.json()["properties"]["state"],
                    job_id=job_id,
                    json=res.json(),
                    _client=self,
                )
            )

    def job_state(self, job_id):
        """Return job state."""
        return self.job(job_id).state

    def jobs(self, geojson=False, indent=4, bounds=None, **kwargs):
        """Return jobs metadata."""
        res = self.get(
            "jobs",
            timeout=self.timeout,
            params=dict(kwargs, bounds=",".join(map(str, bounds)) if bounds else None),
        )
        if res.status_code != 200:  # pragma: no cover
            raise Exception(res.json())
        return (
            json.dumps(res.json(), indent=indent)
            if geojson
            else {
                job["id"]: Job(
                    status_code=200,
                    state=job["properties"]["state"],
                    job_id=job["id"],
                    json=job,
                    _client=self,
                )
                for job in res.json()["features"]
            }
        )

    def jobs_states(self, output_path=None):
        """Return jobs states."""
        return {
            job_id: job.state
            for job_id, job in self.jobs(
                timeout=self.timeout, output_path=output_path
            ).items()
        }

    def job_progress(self, job_id, interval=0.3, wait_for_max=None, raise_exc=True):
        """Yield job progress information."""
        start = time.time()
        last_progress = 0
        while True:
            job = self.job(job_id)
            if (
                wait_for_max is not None and time.time() - start > wait_for_max
            ):  # pragma: no cover
                raise RuntimeError(
                    f"job not done in time, last state was '{job.state}'"
                )
            properties = job.to_dict()["properties"]
            if job.state == "pending":  # pragma: no cover
                continue
            elif job.state == "running" and properties.get("total_progress"):
                current_progress = properties["current_progress"]
                if current_progress > last_progress:
                    yield dict(
                        state=job.state,
                        current_progress=current_progress,
                        total_progress=properties["total_progress"],
                    )
                    last_progress = current_progress
            elif job.state == "aborting":
                if raise_exc:
                    raise JobAborting(f"job {job_id} aborting")
                else:
                    return
            elif job.state == "cancelled":  # pragma: no cover
                if raise_exc:
                    raise JobCancelled(f"job {job_id} cancelled")
                else:
                    return
            elif job.state == "failed":
                if raise_exc:
                    raise JobFailed(f"job failed with {properties['exception']}")
                else:  # pragma: no cover
                    return
            elif job.state == "done":
                current_progress = properties.get("current_progress")
                total_progress = properties.get("total_progress")
                if (current_progress is not None and total_progress is not None) and (
                    current_progress == total_progress
                ):
                    yield dict(
                        state=job.state,
                        current_progress=current_progress,
                        total_progress=total_progress,
                    )
                return
            time.sleep(interval)

    def _get_kwargs(self, kwargs):
        """
        Clean up kwargs.

        For test client:
            - remove timeout kwarg
        """
        if self._test_client:  # pragma: no cover
            kwargs.pop("timeout", None)
        return dict(kwargs, auth=(self._user, self._password))


def load_mapchete_config(mapchete_config, basedir=None):
    """
    Return preprocessed mapchete config provided as dict or file.

    This function reads a mapchete config into an OrderedDict which keeps the item order
    stated in the .mapchete file.
    If the configuration is passed on via a .mapchete file and if a process file path
    instead of a process module path was given, it will also check the syntax and replace
    the process item with the python code as string.

    Parameters
    ----------
    mapchete_config : str or dict
        A valid mapchete configuration either as path or dictionary.

    Returns
    -------
    OrderedDict
        Preprocessed mapchete configuration.
    """
    if isinstance(mapchete_config, (dict)):
        conf = cleanup_datetime(mapchete_config)
    elif isinstance(mapchete_config, str):
        basedir = os.path.dirname(mapchete_config)
        conf = cleanup_datetime(yaml.safe_load(open(mapchete_config, "r").read()))
    else:  # pragma: no cover
        raise TypeError(
            "mapchete config must either be a path to an existing file or a dict"
        )

    process = conf.get("process")

    # handle process
    if not process:  # pragma: no cover
        raise KeyError("no or empty process in configuration")

    if isinstance(process, str):
        # local python file
        if conf.get("process").endswith(".py"):
            custom_process_path = os.path.join(basedir, conf.get("process"))
            # check syntax
            py_compile.compile(custom_process_path, doraise=True)
            # assert file is not empty
            process_code = open(custom_process_path).read()
            if not process_code:  # pragma: no cover
                raise ValueError("process file is empty")
            conf.update(process=process_code.splitlines())

    return conf


def cleanup_datetime(d):
    """Convert datetime objects in dictionary to strings."""
    return OrderedDict(
        (k, cleanup_datetime(v))
        if isinstance(v, dict)
        else (k, str(v))
        if isinstance(v, datetime.date)
        else (k, v)
        for k, v in d.items()
    )
