from __future__ import annotations

import datetime
import json
import logging
import time
from dataclasses import dataclass
from typing import (
    Callable,
    Generator,
    Iterator,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    Union,
)

from mapchete_hub_cli.enums import Status
from mapchete_hub_cli.exceptions import JobCancelled, JobFailed
from mapchete_hub_cli.time import str_to_date
from mapchete_hub_cli.types import Progress

logger = logging.getLogger(__name__)


@dataclass
class Job:
    """Holds job metadata and provides interface with job."""

    status: Status
    job_id: str
    name: str
    geoemtry: dict
    bounds: tuple
    properties: dict
    last_updated: datetime.datetime
    client: JobClientProtocol
    progress: Progress
    _dict: dict
    __geo_interface__: dict

    @staticmethod
    def from_dict(response_dict: dict, client: JobClientProtocol) -> Job:
        return Job(
            status=Status[response_dict["properties"]["status"]],
            job_id=response_dict["id"],
            name=response_dict["properties"]["job_name"],
            geoemtry=response_dict["geometry"],
            bounds=tuple(response_dict["bounds"]),
            properties=response_dict["properties"],
            last_updated=str_to_date(response_dict["properties"]["updated"]),
            client=client,
            progress=Progress(
                current=response_dict["properties"]["current_progress"] or 0,
                total=response_dict["properties"]["total_progress"],
            ),
            _dict=response_dict,
            __geo_interface__=response_dict["geometry"],
        )

    def to_dict(self) -> dict:
        return self._dict

    def to_json(self, indent=4) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def __repr__(self):  # pragma: no cover
        """Print Job."""
        return f"Job(id={self.job_id}, name={self.name}, status={self.status}, last_updated={self.last_updated})"

    def __hash__(self):
        return hash(self.job_id)

    def __eq__(self, other):
        if not isinstance(other, Job):
            return False
        return self.job_id == other.job_id

    def _update(self, job: Job):
        self.status = job.status
        self._dict = job._dict
        self.properties = job.properties
        self.last_updated = job.last_updated
        self.progress = job.progress

    def update(self):
        """Update with remote metadata."""
        self._update(self.client.job(self.job_id))

    def cancel(self):
        """Cancel job."""
        self._update(self.client.cancel_job(self.job_id))
        logger.debug("job %s %s", self.job_id, self.status)

    def retry(self, use_old_image: bool = False) -> Job:
        """Retry and return new job."""
        return self.client.retry_job(self.job_id, use_old_image=use_old_image)

    def wait(self, wait_for_max=None, raise_exc=True):
        """Block until job has finished processing."""
        list(self.yield_progress(wait_for_max=wait_for_max, raise_exc=raise_exc))

    def yield_progress(
        self,
        wait_for_max: Optional[float] = None,
        raise_exc: bool = True,
        interval: float = 0.3,
        smooth: bool = False,
    ) -> Generator[Progress, None, None]:
        """Yield job progress messages."""

        def _progress_iter():
            start = time.time()
            last_progress = 0
            while True:
                self.update()
                if (
                    wait_for_max is not None and time.time() - start > wait_for_max
                ):  # pragma: no cover
                    raise RuntimeError(
                        f"job not done in time, last status was '{self.status}'"
                    )

                # if status is Status.pending, Status.parsing or Status.initializing,
                # we wait and try again
                if self.status == Status.running and self.progress.total:
                    if self.progress.current > last_progress:
                        yield self.progress
                        last_progress = self.progress.current
                elif self.status == Status.cancelled:  # pragma: no cover
                    if raise_exc:
                        raise JobCancelled(f"job {self.job_id} cancelled")
                    else:
                        return
                elif self.status == Status.failed:
                    if raise_exc:
                        raise JobFailed(
                            f"job failed with {self.properties['exception']}"
                        )
                    else:  # pragma: no cover
                        return
                elif self.status == Status.done:
                    if (
                        self.progress.current is not None
                        and self.progress.total is not None
                    ) and (self.progress.current == self.progress.total):
                        yield self.progress
                    return
                time.sleep(interval)

        progress_iter = _progress_iter()

        # to make things smoother, interpolate progress jumps
        if smooth:
            progress = next(progress_iter)
            last_progress = progress.current
            yield progress
            for progress in progress_iter:
                jump = progress.current - last_progress
                for step in range(jump):
                    time.sleep(interval / jump)
                    yield Progress(
                        total=progress.total,
                        current=last_progress + step,
                    )
                last_progress = progress.current
        else:  # pragma: no cover
            yield from progress_iter


@dataclass
class Jobs:
    _jobs: Tuple[Job, ...]
    _response_dict: Optional[dict] = None

    @staticmethod
    def from_dict(response_dict: dict, client: JobClientProtocol) -> Jobs:
        return Jobs(
            _jobs=tuple(
                Job.from_dict(job, client=client) for job in response_dict["features"]
            ),
            _response_dict=response_dict,
        )

    @staticmethod
    def from_jobs(jobs: Union[List[Job], Set[Job]]) -> Jobs:
        return Jobs(_jobs=tuple(jobs))

    def last_job(self) -> Job:
        """Return job which was last updated."""
        return list(sorted(list(self._jobs), key=lambda x: x.last_updated))[-1]

    def cancel(self, msg_writer: Optional[Callable] = None):
        for job in self:
            job.cancel()
            if msg_writer:
                msg_writer(f"job {job.job_id} ({job.name}) {job.status}")

    def _retry(
        self,
        msg_writer: Optional[Callable] = None,
        use_old_image: bool = False,
        cancel: bool = False,
    ) -> Jobs:
        retried_jobs = []
        for job in self:
            if cancel:
                job.cancel()
            retried_job = job.retry(use_old_image=use_old_image)
            retried_jobs.append(retried_job)
            if msg_writer:
                msg_writer(
                    f"job {job.job_id} ({job.name}) {job.status} and retried as {retried_job.job_id} ({retried_job.status})"
                )
        return Jobs.from_jobs(retried_jobs)

    def cancel_and_retry(
        self, msg_writer: Optional[Callable] = None, use_old_image: bool = False
    ) -> Jobs:
        return self._retry(
            msg_writer=msg_writer, use_old_image=use_old_image, cancel=True
        )

    def retry(
        self, msg_writer: Optional[Callable] = None, use_old_image: bool = False
    ) -> Jobs:
        return self._retry(
            msg_writer=msg_writer, use_old_image=use_old_image, cancel=False
        )

    def finished_jobs(self, msg_writer: Optional[Callable] = None) -> Jobs:
        finished_jobs = []
        for job in self:
            if job.status not in [
                Status.done,
                Status.failed,
                Status.cancelled,
            ]:  # pragma: no cover
                if msg_writer:
                    msg_writer(
                        f"Job {job.job_id} ({job.name}) is still in status {job.status}."
                    )
            else:
                finished_jobs.append(job)
        return Jobs.from_jobs(finished_jobs)

    def unfinished_jobs(self, msg_writer: Optional[Callable] = None) -> Jobs:
        unfinished_jobs = []
        for job in self:
            if job.status in [
                Status.done,
                Status.failed,
                Status.cancelled,
            ]:  # pragma: no cover
                if msg_writer:
                    msg_writer(
                        f"Job {job.job_id} ({job.name}) is in status {job.status}."
                    )
            else:
                unfinished_jobs.append(job)
        return Jobs.from_jobs(unfinished_jobs)

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
        if self._response_dict:
            return self._response_dict
        else:  # pragma: no cover
            return {
                "type": "FeatureCollection",
                "features": [job.to_dict() for job in self],
            }

    def to_json(self, indent: int = 4) -> str:
        return json.dumps(self.to_dict(), indent=indent)


class JobClientProtocol(Protocol):  # pragma: no cover
    def cancel_job(self, job_id: str) -> Job: ...

    def retry_job(self, job_id: str, use_old_image: bool = False) -> Job: ...

    def job(self, job_id: str) -> Job: ...
