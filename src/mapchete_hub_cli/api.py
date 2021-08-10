"""
Convenience tools to communicate with mapchete Hub REST API.

This module wraps around the requests module for real-life usage and FastAPI's TestClient()
in order to be able to test mhub CLI.
"""

import base64
from collections import namedtuple, OrderedDict
import datetime
import geojson
import json
import logging
from mapchete.validate import validate_zooms
import os
import py_compile
import requests
from requests.exceptions import ConnectionError
import time
import uuid
import oyaml as yaml

from mapchete_hub_cli.exceptions import JobFailed, JobNotFound, JobRejected


logger = logging.getLogger(__name__)


default_timeout = 5
job_states = {
    "todo": ["pending"],
    "doing": ["running", "aborting"],
    "done": ["done", "failed", "cancelled"]
}
commands = ["execute"]


class Job():
    """Job metadata class."""

    def __init__(
        self, status_code=None, state=None, job_id=None, json=None
    ):
        """Initialize."""
        self.status_code = status_code
        self.state = state
        self.job_id = job_id
        self.exists = True if status_code == 409 else False
        self.json = OrderedDict(json.items())
        self.__geo_interface__ = self.json["geometry"]

    def __repr__(self):  # pragma: no cover
        """Print Job."""
        return f"Job(status_code={self.status_code}, state={self.state}, job_id={self.job_id}, json={self.json}"


Response = namedtuple("Response", "status_code json")


class API():
    """API class which abstracts REST interface."""

    def __init__(self, host="localhost:5000", timeout=None, _test_client=None, **kwargs):
        """Initialize."""
        host = host if host.startswith("http") else f"http://{host}"
        host = host if host.endswith("/") else f"{host}/"
        self.host = host if host.endswith("/") else f"{host}/"
        self.timeout = timeout or default_timeout
        self._test_client = _test_client
        self._api = _test_client if _test_client else requests
        self._baseurl = "" if _test_client else host

    def _request(self, request_type, url, **kwargs):
        _request_func = {
            "GET": self._api.get,
            "POST": self._api.post,
            "PUT": self._api.put,
            "DELETE": self._api.delete,
        }
        if request_type not in _request_func:
            raise ValueError(f"unknown request type '{request_type}'")
        try:
            request_url = self._baseurl + url
            request_kwargs = self._get_kwargs(kwargs)
            logger.debug(f"{request_type}: {request_url}, {request_kwargs}")
            res = _request_func[request_type](request_url, **request_kwargs)
            logger.debug(f"response: {res}")
            return Response(
                status_code=res.status_code,
                json=(
                    res.json if self._test_client else
                    json.loads(res.text, object_pairs_hook=OrderedDict)
                )
            )
        except ConnectionError:  # pragma: no cover
            raise ConnectionError(f"no mhub server found at {self.host}")


    def get(self, url, **kwargs):
        """Make a GET request to _test_client or host."""
        return self._request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        """Make a POST request to _test_client or host."""
        return self._request("POST", url, **kwargs)

    def put(self, url, **kwargs):
        """Make a PUT request to _test_client or host."""
        return self._request("PUT", url, **kwargs)

    def delete(self, url, **kwargs):
        """Make a DELETE request to _test_client or host."""
        return self._request("DELETE", url, **kwargs)

    def start_job(
        self,
        command="execute",
        config=None,
        params=None
    ):
        """
        Start a job and return job state.

        Sends HTTP POST to /jobs/<job_id> and appends mapchete configuration as well
        as processing parameters as JSON.

        Parameters
        ----------
        mapchete_config : path or dict
            Either path to .mapchete file or dictionary with mapchete parameters.
        command : str
            Either "execute" or "index".
        job_id : str (optional)
            Unique job ID.
        bounds : list
            Left, bottom, right, top coordinate of process area.
        point : list
            X and y coordinate of point over process tile.
        tile : list
            Zoom, row and column of process tile.
        geometry : str
            GeoJSON representaion of process area.
        zoom : list or int
            Minimum and maximum zoom level or single zoom level.

        Returns
        -------
        mapchete_hub.api.Job
        """
        job = OrderedDict(
            command=command,
            config=load_mapchete_config(config),
            params=params or {}
        )

        # make sure correct command is provided
        if command not in commands:
            raise ValueError(f"invalid command given: {command}")

        logger.debug(f"send job to API")
        res = self.post(
            f"processes/{command}/execution",
            data=json.dumps(job),
            timeout=self.timeout
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
                json=res.json()
            )

    def cancel_job(self, job_id):
        """Cancel existing job."""
        res = self.delete(
            f"jobs/{job_id}",
            timeout=self.timeout
        )
        if res.status_code == 404:
            raise JobNotFound(f"job {job_id} does not exist")
        return Job(
            status_code=res.status_code,
            state=self.job_state(job_id),
            job_id=job_id,
            json=res.json()
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
            config=existing_job.json["properties"]["mapchete"]["config"],
            command=existing_job.json["properties"]["mapchete"]["command"],
            params=existing_job.json["properties"]["mapchete"]["params"]
        )

    def job(self, job_id, geojson=False):
        """Return job metadata."""
        res = self.get(f"jobs/{job_id}", timeout=self.timeout)
        if res.status_code == 404:
            raise JobNotFound(f"job {job_id} does not exist")
        else:
            return (
                format_as_geojson(res.json())
                if geojson
                else Job(
                    status_code=res.status_code,
                    state=res.json()["properties"]["state"],
                    job_id=job_id,
                    json=res.json()
                )
            )

    def job_state(self, job_id):
        """Return job state."""
        return self.job(job_id).state

    def jobs(self, geojson=False, bounds=None, **kwargs):
        """Return jobs metadata."""
        res = self.get(
            "jobs",
            timeout=self.timeout,
            params=dict(
                kwargs,
                bounds=",".join(map(str, bounds)) if bounds else None
            )
        )
        if res.status_code != 200:  # pragma: no cover
            raise Exception(res.json())
        return (
            format_as_geojson(res.json())
            if geojson
            else {
                job["id"]: Job(
                    status_code=200,
                    state=job["properties"]["state"],
                    job_id=job["id"],
                    json=job
                )
                for job in res.json()
            }
        )

    def jobs_states(self, output_path=None):
        """Return jobs states."""
        return {
            job["properties"]["job_id"]: job["properties"]["state"]
            for job in self.get(
                "jobs",
                timeout=self.timeout,
                params=dict(output_path=output_path)
            ).json()
        }

    def job_progress(self, job_id, interval=1, timeout=None):
        """Yield job progress information."""
        last = -1
        updated = time.time()
        while True:
            job = self.job(job_id)
            logger.debug(job.state)

            if job.state in job_states["todo"]:
                pass

            if job.state in job_states["doing"]:
                if job.state in ["RECEIVED", "STARTED"]:
                    pass
                if job.state in ["PROGRESS"]:
                    logger.debug(job.json())
                    x = job.json()["properties"]["progress_data"].get("current", None)
                    current = -1 if x is None else x
                    if current > last:
                        last = job.json()["properties"]["progress_data"]["current"]
                        updated = time.time()
                        yield job.json()["properties"]

            if job.state in job_states["done"]:
                if job.state == "SUCCESS":
                    yield job.json()["properties"]
                    return
                if job.state == "FAILURE":  # pragma: no cover
                    raise JobFailed(job.json()["properties"]["traceback"])

            if timeout is not None and time.time() - updated > timeout:
                raise TimeoutError(f"no update since {timeout} seconds")

            time.sleep(interval)

    def _get_kwargs(self, kwargs):
        """
        Clean up kwargs.

        For test client:
            - remove timeout kwarg
        """
        if self._test_client:  # pragma: no cover
            kwargs.pop("timeout", None)
        return kwargs

    def _batch_config_from_job(self, job):
        def _next_job(job):
            yield job
            next_job_id = job.json()["properties"].get("next_job_id")
            if next_job_id:
                yield from _next_job(self.job(next_job_id))

        def _param_not_empty(p):
            if p is None:
                return False
            if isinstance(p, (list, tuple)) and not len(p):
                return False
            return True


        def _job_to_batch(job):
            """
            properties/mapchete/config --> mapchete
            properties/mapchete/params --> root
            """
            return OrderedDict(
                mapchete=job.json()["properties"]["mapchete"]["config"],
                command=job.json()["properties"]["mapchete"]["command"],
                **{
                    k: v
                    for k, v in job.json()["properties"]["mapchete"]["params"].items()
                    if _param_not_empty(v)
                }
            )

        return dict(
            jobs=OrderedDict(
                (job.json()["properties"]["job_name"], _job_to_batch(job))
                for job in _next_job(job)
            )
        )


def format_as_geojson(inp, indent=4):
    """Return a pretty GeoJSON."""
    space = " " * indent
    out_gj = (
        '{{\n'
        f'{space}"type": "FeatureCollection",\n'
        f'{space}"features": [\n'
    )
    features = (i for i in ([inp] if isinstance(inp, dict) else inp))
    try:
        feature = next(features)
        level = 2
        while True:
            feature_gj = (space * level).join(
                json.dumps(
                    json.loads(
                        str(geojson.Feature(**feature)), object_pairs_hook=OrderedDict
                    ),
                    indent=indent,
                    sort_keys=True
                ).splitlines(True)
            )
            try:
                feature = next(features)
                out_gj += f"{space * level}{feature_gj},\n"
            except StopIteration:
                out_gj += f"{space * level}{feature_gj}\n"
                break
    except StopIteration:  # pragma: no cover
        pass
    out_gj += f"{space}]\n}}"
    return out_gj


def load_mapchete_config(mapchete_config):
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
        return cleanup_datetime(mapchete_config)

    elif isinstance(mapchete_config, str):
        conf = cleanup_datetime(yaml.safe_load(open(mapchete_config, "r").read()))

        if not conf.get("process"):  # pragma: no cover
            raise KeyError("no or empty process in configuration")

        # local python file
        if conf.get("process").endswith(".py"):
            custom_process_path = os.path.join(
                os.path.dirname(mapchete_config),
                conf.get("process")
            )
            # check syntax
            py_compile.compile(custom_process_path, doraise=True)
            # assert file is not empty
            process_code = open(custom_process_path).read()
            if not process_code:  # pragma: no cover
                raise ValueError("process file is empty")
            conf.update(
                process=base64.standard_b64encode(
                    process_code.encode("utf-8")
                ).decode("utf-8")
            )

        return conf

    else:  # pragma: no cover
        raise TypeError(
            "mapchete config must either be a path to an existing file or a dict"
        )


def cleanup_datetime(d):
    """Convert datetime objects in dictionary to strings."""
    return OrderedDict(
        (k, cleanup_datetime(v)) if isinstance(v, dict)
        else (k, str(v)) if isinstance(v, datetime.date) else (k, v)
        for k, v in d.items()
    )
