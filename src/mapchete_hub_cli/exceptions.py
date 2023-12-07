class UnknownJobStatus(Exception):
    """Raise if job is registered but has unknown status."""


class JobNotStarted(Exception):
    """Raise if job is registered but has not started yet."""


class JobFailed(Exception):
    """Raise if job is registered but has failed."""


class JobNotFound(Exception):
    """Raise if job could not be found."""


class JobRejected(Exception):
    """Raise if job is rejected from server."""


class JobAborting(Exception):
    """Raise if job is cancelled."""


class JobCancelled(Exception):
    """Raise if job is cancelled."""
