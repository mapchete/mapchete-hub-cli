"""
Convenience tools to communicate with mapchete Hub REST API.

This module wraps around the requests module for real-life usage and FastAPI's TestClient()
in order to be able to test mhub CLI.
"""
from __future__ import annotations

import datetime
import json
import logging
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from json.decoder import JSONDecodeError
from typing import Generator, Iterator, List, Optional, Tuple, Union

import requests
from requests.exceptions import HTTPError

from mapchete_hub_cli.enums import Status
from mapchete_hub_cli.exceptions import (
    JobCancelled,
    JobFailed,
    JobNotFound,
    JobRejected,
)
from mapchete_hub_cli.parser import load_mapchete_config
from mapchete_hub_cli.time import date_to_str, passed_time_to_timestamp, str_to_date

logger = logging.getLogger(__name__)

MHUB_CLI_ZONES_WAIT_TILES_COUNT = int(
    os.environ.get("MHUB_CLI_ZONES_WAIT_TILES_COUNT", "5")
)
MHUB_CLI_ZONES_WAIT_TIME_SECONDS = int(
    os.environ.get("MHUB_CLI_ZONES_WAIT_TIME_SECONDS", "10")
)
DEFAULT_TIMEOUT = int(os.environ.get("MHUB_CLI_DEFAULT_TIMEOUT", "5"))

JOB_STATUSES = {
    "todo": [Status.pending],
    "doing": [Status.parsing, Status.initializing, Status.retrying, Status.running],
    "done": [Status.done, Status.failed, Status.cancelled],
}
COMMANDS = ["execute"]


@dataclass
class Job:
    """Job metadata class."""

    status: Status
    job_id: str
    geoemtry: dict
    __geo_interface__: dict
    bounds: tuple
    properties: dict
    last_updated: datetime.datetime
    client: Optional[Client]
    _dict: dict

    @staticmethod
    def from_dict(response_dict: dict, client: Optional[Client] = None) -> Job:
        return Job(
            status=Status[response_dict["properties"]["status"]],
            job_id=response_dict["id"],
            geoemtry=response_dict["geometry"],
            __geo_interface__=response_dict["geometry"],
            bounds=tuple(response_dict["bounds"]),
            properties=response_dict["properties"],
            last_updated=str_to_date(response_dict["properties"]["updated"]),
            client=client,
            _dict=response_dict,
        )

    def to_dict(self) -> dict:
        return self._dict

    def to_json(self, indent=4) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def __repr__(self):  # pragma: no cover
        """Print Job."""
        return f"Job(status={self.status}, job_id={self.job_id}, updated={self.last_updated}"

    def wait(self, wait_for_max=None, raise_exc=True):
        """Block until job has finished processing."""
        list(self.progress(wait_for_max=wait_for_max, raise_exc=raise_exc))

    def progress(self, wait_for_max=None, raise_exc=True, interval=0.3, smooth=False):
        """Yield job progress messages."""
        progress_iter = self.client.job_progress(
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


@dataclass
class Jobs:
    client: Client
    _response_dict: dict
    _jobs: Tuple[Job, ...]

    @staticmethod
    def from_dict(response_dict: dict, client: Client) -> Jobs:
        return Jobs(
            client=client,
            _jobs=tuple(
                Job.from_dict(job, client=client) for job in response_dict["features"]
            ),
            _response_dict=response_dict,
        )

    def last_job(self) -> Job:
        return list(sorted(list(self._jobs), key=lambda x: x.last_updated))[-1]

    def __iter__(self) -> Iterator[Job]:
        return iter(self._jobs)

    def __len__(self) -> int:
        return len(self._jobs)

    def __getitem__(self, job_id: str) -> Job:
        for _job in self._jobs:
            if job_id == _job.job_id:
                return _job
        else:
            raise KeyError(f"job with id {job_id} not in {self}")

    def __contains__(self, job: Union[Job, str]) -> bool:
        job_id = job.job_id if isinstance(job, Job) else job
        try:
            self[job_id]
            return True
        except KeyError:
            return False

    def to_dict(self) -> dict:
        return self._response_dict

    def to_json(self, indent: int = 4) -> str:
        return json.dumps(self.to_dict(), indent=indent)


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
        self.timeout = timeout or DEFAULT_TIMEOUT
        self._user = user or os.environ.get("MHUB_USER")
        self._password = password or os.environ.get("MHUB_PASSWORD")
        self._test_client = _test_client
        self._client = _test_client if _test_client else requests
        self._baseurl = "" if _test_client else host

    @property
    def remote_version(self):
        response = self.get("", timeout=self.timeout).json()
        return response.get("versions", response.get("title", "").split(" ")[-1])

    def _request(self, request_type: str, url: str, **kwargs) -> requests.Response:
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
            start = time.time()
            response = _request_func[request_type](request_url, **request_kwargs)
            end = time.time()
            logger.debug(f"response: {response}")
            logger.debug(f"response took {round(end - start, 3)}s")
            if response.status_code == 401:  # pragma: no cover
                raise HTTPError("Authorization failure")
            elif response.status_code >= 500:  # pragma: no cover
                logger.error(f"response text: {response.text}")
            return response
        except ConnectionError:  # pragma: no cover
            raise ConnectionError(f"no mhub server found at {self.host}")

    def get(self, url: str, **kwargs) -> requests.Response:
        """Make a GET request to _test_client or host."""
        return self._request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        """Make a POST request to _test_client or host."""
        return self._request("POST", url, **kwargs)

    def delete(self, url: str, **kwargs) -> requests.Response:
        """Make a DELETE request to _test_client or host."""
        return self._request("DELETE", url, **kwargs)

    def get_last_job_id(self, since: str = "1d") -> str:
        """
        Return ID of latest job if requested.
        """
        jobs = self.jobs(from_date=passed_time_to_timestamp(since))
        if len(jobs) == 0:  # pragma: no cover
            raise JobNotFound(f"cannot find recent job since {since}")
        return jobs.last_job().job_id

    def parse_job_id(self, job_id: str) -> str:
        if job_id == ":last:":
            return self.get_last_job_id()
        return job_id

    def start_job(
        self,
        config: dict,
        params: Optional[dict] = None,
        command: str = "execute",
        basedir: Optional[str] = None,
    ) -> Job:
        """
        Start a job and return job status.

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
        if params is None:
            params = {}
        elif isinstance(params, dict):
            params = {k: v for k, v in params.items() if v is not None}
        else:
            raise JobRejected(f"params must be None or a dictionary, not '{params}'")

        job = OrderedDict(
            command=command,
            config=load_mapchete_config(config, basedir=basedir),
            params=params,
        )
        # make sure correct command is provided
        if command not in COMMANDS:  # pragma: no cover
            raise ValueError(f"invalid command given: {command}")

        logger.debug("send job to API")
        res = self.post(
            f"processes/{command}/execution",
            data=json.dumps(job, default=str),
            timeout=self.timeout,
        )

        if res.status_code != 201:
            try:
                raise JobRejected(res.json())
            except JSONDecodeError:  # pragma: no cover
                raise Exception(res.text)
        else:
            job_id = res.json()["id"]
            logger.debug(f"job {job_id} sent")
            return Job.from_dict(res.json(), client=self)

    def cancel_job(self, job_id: str) -> Job:
        """
        Cancel existing job.

        Parameters
        ----------
        job_id : str
            Can either be a valid job ID or :last:, in which case the CLI will automatically
            determine the most recently updated job.
        """
        job_id = self.parse_job_id(job_id)
        res = self.delete(f"jobs/{job_id}", timeout=self.timeout)
        if res.status_code == 404:
            raise JobNotFound(f"job {job_id} does not exist")
        return Job.from_dict(res.json(), client=self)

    def retry_job(self, job_id: str, use_old_image: bool = False) -> Job:
        """
        Retry a job and its children and return job status.

        Sends HTTP POST to /jobs/<job_id> and appends mapchete configuration as well
        as processing parameters as JSON.

        Parameters
        ----------
        job_id : str
            Can either be a valid job ID or :last:, in which case the CLI will automatically
            determine the most recently updated job.

        Returns
        -------
        mapchete_hub.api.Job
        """
        existing_job = self.job(self.parse_job_id(job_id))
        params = existing_job.properties["mapchete"]["params"].copy()
        if not use_old_image:
            # make sure to remove image from params because otherwise the job will be retried
            # using outdated software
            try:
                params["dask_specs"].pop("image")
            except KeyError:
                pass
        return self.start_job(
            config=existing_job.properties["mapchete"]["config"],
            command=existing_job.properties["mapchete"]["command"],
            params=params,
        )

    def job(self, job_id: str) -> Job:
        """
        Return job metadata.

        Parameters
        ----------
        job_id : str
            Can either be a valid job ID or :last:, in which case the CLI will automatically
            determine the most recently updated job.
        """
        job_id = self.parse_job_id(job_id)
        res = self.get(f"jobs/{job_id}", timeout=self.timeout)
        if res.status_code == 200:
            return Job.from_dict(res.json(), client=self)
        elif res.status_code == 404:
            raise JobNotFound(f"job {job_id} does not exist")
        else:
            raise ValueError(f"return code should be 200, but is {res.status_code}")

    def job_status(self, job_id: str) -> Status:
        """
        Return job status.

        Parameters
        ----------
        job_id : str
            Can either be a valid job ID or :last:, in which case the CLI will automatically
            determine the most recently updated job.
        """
        return self.job(self.parse_job_id(job_id)).status

    def jobs(
        self,
        bounds: Optional[Union[List, Tuple]] = None,
        from_date: Optional[Union[str, datetime.datetime]] = None,
        to_date: Optional[Union[str, datetime.datetime]] = None,
        status: Optional[Union[List[Status], Status]] = None,
        **kwargs,
    ) -> Jobs:
        res = self.get(
            "jobs",
            timeout=self.timeout,
            params=dict(
                kwargs,
                bounds=",".join(map(str, bounds)) if bounds else None,
                from_date=date_to_str(from_date) if from_date else None,
                to_date=date_to_str(to_date) if to_date else None,
                status=",".join([s.name for s in to_statuses(status)])
                if status
                else None,
            ),
        )
        if res.status_code != 200:  # pragma: no cover
            try:
                raise Exception(res.json())
            except JSONDecodeError:  # pragma: no cover
                raise Exception(res.text)
        return Jobs.from_dict(res.json(), client=self)

    def job_progress(
        self,
        job_id: str,
        interval: float = 0.3,
        wait_for_max: Optional[float] = None,
        raise_exc: bool = True,
    ) -> Generator[dict, None, None]:
        """
        Yield job progress information.

        Parameters
        ----------
        job_id : str
            Can either be a valid job ID or :last:, in which case the CLI will automatically
            determine the most recently updated job.
        """
        start = time.time()
        last_progress = 0
        while True:
            job = self.job(self.parse_job_id(job_id))
            if (
                wait_for_max is not None and time.time() - start > wait_for_max
            ):  # pragma: no cover
                raise RuntimeError(
                    f"job not done in time, last status was '{job.status}'"
                )
            properties = job.to_dict()["properties"]
            if job.status == Status.pending:  # pragma: no cover
                continue
            elif job.status == Status.running and properties.get("total_progress"):
                current_progress = properties["current_progress"]
                if current_progress > last_progress:
                    yield dict(
                        status=job.status,
                        current_progress=current_progress,
                        total_progress=properties["total_progress"],
                    )
                    last_progress = current_progress
            elif job.status == Status.cancelled:  # pragma: no cover
                if raise_exc:
                    raise JobCancelled(f"job {job.job_id} cancelled")
                else:
                    return
            elif job.status == Status.failed:
                if raise_exc:
                    raise JobFailed(f"job failed with {properties['exception']}")
                else:  # pragma: no cover
                    return
            elif job.status == Status.done:
                current_progress = properties.get("current_progress")
                total_progress = properties.get("total_progress")
                if (current_progress is not None and total_progress is not None) and (
                    current_progress == total_progress
                ):
                    yield dict(
                        status=job.status,
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
        if self._user is not None and self._password is not None:
            kwargs.update(auth=(self._user, self._password))
        return kwargs

    def __repr__(self):  # pragma: no cover
        return f"Client(host={self.host}, user={self._user}, password={self._password})"


def to_statuses(status: Optional[Union[List[Status], Status]] = None) -> List[Status]:
    def to_status(s: Union[str, Status]) -> Status:
        return s if isinstance(s, Status) else Status[s]

    if status is None:
        return []
    elif isinstance(status, list):
        return [to_status(s) for s in status]
    else:
        return [to_status(status)]
