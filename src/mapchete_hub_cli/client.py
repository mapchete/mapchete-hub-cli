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
from json.decoder import JSONDecodeError
from typing import Callable, List, Optional, Tuple, Union

import requests
from requests.exceptions import HTTPError

from mapchete_hub_cli.enums import Status
from mapchete_hub_cli.exceptions import JobNotFound, JobRejected
from mapchete_hub_cli.job import Job, Jobs
from mapchete_hub_cli.parser import load_mapchete_config
from mapchete_hub_cli.time import (
    date_to_str,
    passed_time_to_timestamp,
    pretty_time_since,
)

logger = logging.getLogger(__name__)

MHUB_CLI_ZONES_WAIT_TILES_COUNT = int(
    os.environ.get("MHUB_CLI_ZONES_WAIT_TILES_COUNT", "100")
)
MHUB_CLI_ZONES_WAIT_TIME_SECONDS = float(
    os.environ.get("MHUB_CLI_ZONES_WAIT_TIME_SECONDS", "1")
)
DEFAULT_TIMEOUT = int(os.environ.get("MHUB_CLI_DEFAULT_TIMEOUT", "10"))

JOB_STATUSES = {
    "todo": [Status.pending],
    "doing": [Status.parsing, Status.initializing, Status.retrying, Status.running],
    "done": [Status.done, Status.failed, Status.cancelled],
}
COMMANDS = ["execute"]


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
        config: Union[dict, str],
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
            headers={"Content-Type": "application/json"},
        )

        if res.status_code != 201:  # pragma: no cover
            try:
                raise JobRejected(res.json())
            except JSONDecodeError:
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
            except KeyError:  # pragma: no cover
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
        else:  # pragma: no cover
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
        unique_by_job_name: bool = False,
        **kwargs,
    ) -> Jobs:
        """
        Query interface for jobs.
        """
        statuses = [s.name for s in to_statuses(status)] if status else []
        query_params = dict(
            kwargs,
            bounds=",".join(map(str, bounds)) if bounds else None,
            from_date=date_to_str(from_date) if from_date else None,
            to_date=date_to_str(to_date) if to_date else None,
            status=",".join(statuses) if statuses else None,
        )

        # we need to filter out jobs by statuses *after* we created a unique list
        if unique_by_job_name:
            query_params.pop("status")

        res = self.get(
            "jobs",
            timeout=self.timeout,
            params=query_params,
        )
        if res.status_code != 200:  # pragma: no cover
            try:
                raise Exception(res.json())
            except JSONDecodeError:
                raise Exception(res.text)

        jobs = res.json()

        if unique_by_job_name:
            unique_jobs: OrderedDict = OrderedDict()
            for job_feature in jobs["features"]:
                job = Job.from_dict(job_feature, client=self)
                if job.name in unique_jobs:
                    existing_job = unique_jobs[job.name]
                    if job.last_updated > existing_job.last_updated:
                        unique_jobs[job.name] = job
                else:
                    unique_jobs[job.name] = job
            if statuses:
                jobs["features"] = [
                    job.to_dict()
                    for job in unique_jobs.values()
                    if job.status.name in statuses
                ]
            else:
                jobs["features"] = [job.to_dict() for job in unique_jobs.values()]

        return Jobs.from_dict(jobs, client=self)

    def stalled_jobs(
        self,
        inactive_since: str = "5h",
        pending_since: str = "3d",
        check_inactive_dashboard: bool = True,
        msg_writer: Optional[Callable] = None,
    ) -> Jobs:
        stalled = set()

        # jobs which have been pending for too long
        for job in self.jobs(
            status=Status.pending,
            to_date=date_to_str(passed_time_to_timestamp(pending_since)),
        ):  # pragma: no cover
            logger.debug(
                "job %s %s state since %s", job.job_id, job.status, job.last_updated
            )
            if msg_writer:
                msg_writer(
                    f"{job.job_id} {job.status} since {pretty_time_since(job.last_updated)}"
                )
            stalled.add(job)

        # jobs which have been inactive for too long
        for status in [Status.parsing, Status.initializing, Status.running]:
            for job in self.jobs(
                status=status,
                to_date=date_to_str(passed_time_to_timestamp(inactive_since)),
            ):  # pragma: no cover
                logger.debug(
                    "job %s %s but has been inactive since %s",
                    job.job_id,
                    job.status,
                    job.last_updated,
                )
                if msg_writer:
                    msg_writer(
                        f"{job.job_id} {job.status} but has been inactive since {pretty_time_since(job.last_updated)}"
                    )
                stalled.add(job)

        # running jobs with unavailable dashboard
        if check_inactive_dashboard:
            for job in self.jobs(status=Status.running):  # pragma: no cover
                dashboard_link = job.properties.get("dask_dashboard_link")
                # NOTE: jobs can be running without haveing a dashboard
                if dashboard_link:
                    status_code = requests.get(dashboard_link).status_code
                    if status_code != 200:
                        logger.debug(
                            "job %s %s but dashboard %s returned status code %s",
                            job.job_id,
                            job.status,
                            dashboard_link,
                            status_code,
                        )
                        if msg_writer:
                            msg_writer(
                                f"{job.job_id} {job.status} but has inactive dashboard (status code {status_code}, job inactive since {pretty_time_since(job.last_updated)})"
                            )
                        stalled.add(job)

        return Jobs.from_jobs(stalled)

    def _get_kwargs(self, kwargs):
        """
        Clean up kwargs.

        For test client:
            - remove timeout kwarg
        """
        if self._test_client:  # pragma: no cover
            kwargs.pop("timeout", None)
        if self._user is not None and self._password is not None:  # pragma: no cover
            kwargs.update(auth=(self._user, self._password))
        return kwargs

    def __repr__(self):  # pragma: no cover
        return f"Client(host={self.host}, user={self._user}, password={self._password})"


def to_statuses(status: Optional[Union[List[Status], Status]] = None) -> List[Status]:
    def to_status(s: Union[str, Status]) -> Status:
        return s if isinstance(s, Status) else Status[s]

    status = status or []
    if isinstance(status, list):  # pragma: no cover
        return [to_status(s) for s in status]
    else:
        return [to_status(status)]
